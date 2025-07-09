"""
Created on 27-May-2025

@author: himanshu.jain@couchbase.com
"""

from queue import Queue
import random
import threading
import time

from Columnar.columnar_base import ColumnarBaseTest
from Columnar.columnar_rbac_cloud import generate_random_entity_name
from kafka_util.confluent_utils import ConfluentUtils
from kafka_util.kafka_connect_util import KafkaConnectUtil
from cbas_utils.cbas_utils_columnar import CBOUtil

from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from Jython_tasks.sirius_task import CouchbaseUtil
from common_lib import sleep


class ColumnFilter(ColumnarBaseTest):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.pod = None
        self.tenant = None
        self.no_of_docs = None
        self.initial_insert_completed = threading.Event()

    def setUp(self):
        super(ColumnFilter, self).setUp()
        self.columnar_cluster = self.tenant.columnar_instances[0]
        self.remote_cluster = None
        if len(self.tenant.clusters) > 0:
            self.remote_cluster = self.tenant.clusters[0]
            self.couchbase_doc_loader = CouchbaseUtil(
                task_manager=self.task_manager,
                hostname=self.remote_cluster.master.ip,
                username=self.remote_cluster.master.rest_username,
                password=self.remote_cluster.master.rest_password,
            )

        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.doc_size = self.input.param("doc_size", 1024)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"
        self.columnar_spec = self.cbas_util.get_columnar_spec(self.columnar_spec_name)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)


    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.columnar_cluster):
            self.fail("Error while deleting cbas entities")

        if hasattr(self, "remote_cluster") and self.remote_cluster:
            self.delete_all_buckets_from_capella_cluster(
                self.tenant, self.remote_cluster)

        # super(ColumnFilter, self).tearDown()

        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")


    def test_setup(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster)
        
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster)

        # create remote link and remote collection in columnar
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master, self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)

        remote_links = self.cbas_util.get_all_link_objs("couchbase")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to connect link")

    
    

    def get_random_interval(self, total_docs, interval_size):
        """
        Generate a random interval of specified size within the range [0, total_docs)
        
        Args:
            total_docs: Total number of documents
            interval_size: Size of the interval to generate
            
        Returns:
            tuple: (start_index, end_index) where end_index is exclusive
        """
        if interval_size >= total_docs:
            # If interval size is >= total docs, use the full range
            return 0, total_docs
        
        # Generate a random start position
        max_start = total_docs - interval_size
        start_index = random.randint(0, max_start)
        end_index = start_index + interval_size
        
        return start_index, end_index
    
    def validate(self, num_of_items):
        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)
        
        
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        for dataset in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, dataset.full_name,
                    num_of_items):
                self.fail("Doc count mismatch.")

    def data_mutation_job(self):
        """Job function for data mutation operations"""
        iteration = 0
        while True:
            iteration += 1
            self.log.info(f"Iteration data_mutation_job {iteration}")
            # Upsert 1M docs
            self.log.info(f"Upserting {self.initial_doc_count} docs in remote collection")
            self.load_doc_to_remote_collections(self.remote_cluster,"HeterogeneousHotel",
                                    update_start_index=0, update_end_index=self.initial_doc_count, update_percent=100, create_percent=0)
        
            self.validate(self.initial_doc_count)

            # Signal that initial insert is completed
            self.initial_insert_completed.set()

            # Delete 50% docs with random interval
            interval_size = self.initial_doc_count // 2
            delete_start_index, delete_end_index = self.get_random_interval(
                self.initial_doc_count, interval_size)
            self.log.info(f"Deleting from {delete_start_index} to {delete_end_index}. Total deleted docs: {delete_end_index - delete_start_index}")
            self.load_doc_to_remote_collections(self.remote_cluster,"HeterogeneousHotel",
                                    delete_start_index=delete_start_index, delete_end_index=delete_end_index, delete_percent=100, create_percent=0)
        
            self.validate(interval_size)
            # self.initial_insert_completed.clear()
            print("--------------------------------")

    def query_execution_job(self):
        """Job function for query execution"""
        iteration = 0
        while True:
            iteration += 1
            self.log.info(f"Iteration query_execution_job {iteration}")
            # Wait for initial insert to complete before starting query execution
            # self.log.info("Waiting for initial upsert to complete before starting query execution")
            self.initial_insert_completed.wait()
            # self.log.info("Upsert completed, starting query execution")
            
            dataset = self.cbas_util.get_all_dataset_objs("remote")[0]
            cmd = f"SET `compiler.column.filter` \"true\"; select count(*) from {dataset.name} where price between 1000 and 1200;"
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success" or len(results) == 0:
                self.fail(f"Failed to run the query: {cmd}")
            self.log.info(f"Count: {results}")
            self.log.info("Sleeping for 30 seconds")
            time.sleep(30)
            print("--------------------------------")

    """
    Thread 1: data_mutation_job
        upsert 1M
        delete 50%
    Thread 2: query_execution_job
        run query during upsert and delete
    """
    def test_mutate_data(self):
        self.test_setup()

        jobs = Queue()
        results = []

        jobs.put((self.data_mutation_job,{}))
        jobs.put((self.query_execution_job,{}))

        self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False
            )

        if not all(results):
            self.fail("Test failed")
        self.log.info("Test passed")
        return