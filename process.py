
import argparse
import asyncio
import re
import aiohttp
import logging
import sys
import itertools
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)


class Factory:
    def __init__(self, subscription_id, resource_group, name):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.name = name
        self.pipelines = []
        self.dataflows = set()
        self.datasets = {}
        self.linkservices = {}
        self.log = logger.getChild(__class__.__name__)

    def add_pipeline(self, pipeline):
        self.pipelines.append(pipeline)

    def add_pipelines(self, pipelines):
        self.pipelines.extend(pipelines)

    def add_dataflow(self, reference_name):
        self.dataflows.add(reference_name)

    def add_dataset(self, reference_name):
        self.datasets[reference_name] = {}

    def add_dataset_info(self, reference_name, info):
        self.datasets[reference_name] = info

    def add_linkservice(self, reference_name):
        self.linkservices[reference_name] = {}

    def add_linkservice_info(self, reference_name, info):
        self.linkservices[reference_name] = info

    def __str__(self):
        return f"Factory: {self.name} {self.subscription_id} {self.resource_group}"
    
    def __repr__(self) -> str:
        return f"Factory = {{ name={self.name}, subscription_id={self.subscription_id}, resource_group={self.resource_group}, pipelines={self.pipelines}, dataflows={self.dataflows}, datasets={self.datasets}, linkedservices={self.linkservices} }}"
    
class Pipeline:
    def __init__(self, name):
        self.name = name
        self.activities = []

    def add_activity(self, activity):
        self.activities.append(activity)

    def __str__(self):
        return f"Pipeline: {self.name} {self.pipeline}"
    
    def __repr__(self) -> str:
        return f"{{ Pipeline: {self.name} {self.activities} }}"
    
async def get_factories(subscription_id, headers, session):
    log = logger.getChild(get_factories.__name__)
    log.debug(f"processing subscription_id {subscription_id}")
    url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.DataFactory/factories?api-version=2018-06-01"
    try:        
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                response_data = await response.json()
                log.info(f"Subscription: {subscription_id} processed successfully.")
                values = response_data.get("value")
                factories = []
                for v in values:
                    name = v.get("name")
                    id = v.get("id")
                    match = re.search(r"resourceGroups/([^/]+)/providers", id)
                    factory = Factory(subscription_id, match.group(1), name)
                    factory.add_pipelines(await get_pipelines(factory, headers, session))
                    factories.append(factory)
                return factories
            else:
                text = await response.text()
                log.error(f"Subscription: {subscription_id}, Response Code: {response.status} Error: {text}")
                return []
    except asyncio.TimeoutError as e:
        log.error(f"Timeout error for {subscription_id}")
        return []
    except Exception as e:
        log.error(f"Unable to get url {url} ({subscription_id}) due to {e}")
        return []
    
async def handle_activity(activity, factory):
    log = logger.getChild(handle_activity.__name__)
    activity_name = activity.get("name")
    log.debug(f"processing activity {activity_name}")
    activity_type = activity.get("type")
    if activity_type == 'ExecuteDataFlow':
        type_props = activity.get("typeProperties")
        df = type_props.get("dataflow")
        ref_name = df.get("referenceName")
        df_type = df.get("type")
        if df_type == 'DataFlowReference':
            factory.add_dataflow(ref_name)
        else:
            log.debug(f"ExecuteDataFlow DataFlow type {df_type} not handled")
    elif activity_type == 'Copy':
        inputs = activity.get("inputs")
        outputs = activity.get("outputs")
        for io in itertools.chain(inputs, outputs):
            ref_type = io.get("type")
            ref_name = io.get("referenceName")
            if ref_type == 'DatasetReference':
                factory.add_dataset(ref_name)
            else:
                log.debug(f"Copy DataSet type {ref_type} not handled")
    elif activity_type == 'Lookup':
        type_props = activity.get("typeProperties")
        ds = type_props.get("dataset")
        ref_name = ds.get("referenceName")
        ref_type = ds.get("type")
        if ref_type == 'DatasetReference':
            factory.add_dataset(ref_name)
        else:
            log.debug(f"Lookup DataSet type {ref_type} not handled")
    else:
        log.debug(f"Activity type {activity_type} not handled")
    
async def get_pipelines(factory, headers, session):
    log = logger.getChild(get_pipelines.__name__)
    log.debug(f"processing subscription_id:factory {factory.subscription_id}:{factory.name}")
    url = f"https://management.azure.com/subscriptions/{factory.subscription_id}/resourceGroups/{factory.resource_group}/providers/Microsoft.DataFactory/factories/{factory.name}/pipelines?api-version=2018-06-01"
    try:        
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                response_data = await response.json()
                log.info(f"Factory {factory.subscription_id}:{factory.name} processed successfully.")
                values = response_data.get("value")
                result = []
                for v in values:
                    name = v.get("name")
                    pipeline = Pipeline(name)
                    factory.add_pipeline(pipeline)
                    properties = v.get("properties")
                    activities = properties.get("activities")
                    for activity in activities:
                        await handle_activity(activity, factory)
                return result
            else:
                text = await response.text()
                log.error(f"Subscription: {factory.subscription_id}, Response Code: {response.status} Error: {text}")
                return []
    except asyncio.TimeoutError as e:
        log.error(f"Timeout error for {factory.subscription_id}:{factory}")
        return []
    except Exception as e:
        log.error(f"Unable to get url {url} ({factory.subscription_id}:{factory}) due to {e}")
        return []

async def get_dataflows(factory, dataflow, headers, session):
    log = logger.getChild(get_dataflows.__name__)
    log.debug(f"processing subscription_id:factory:dataflow {factory.subscription_id}:{factory.name}:{dataflow}")
    url = f"https://management.azure.com/subscriptions/{factory.subscription_id}/resourceGroups/{factory.resource_group}/providers/Microsoft.DataFactory/factories/{factory.name}/dataflows/{dataflow}?api-version=2018-06-01"
    try:        
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                response_data = await response.json()
                log.info(f"Dataflow {factory.subscription_id}:{factory.name}:{dataflow} processed successfully.")
                props = response_data.get("properties")
                type_props = props.get("typeProperties")
                sources = type_props.get("sources")
                sinks = type_props.get("sinks")
                dataset_ref_names = []
                for ss in itertools.chain(sources, sinks):
                    ds = ss.get("dataset")
                    ref_name = ds.get("referenceName")
                    dataset_ref_names.append(ref_name)
                return (factory, dataset_ref_names)
            else:
                text = await response.text()
                log.error(f"Subscription: {factory.subscription_id}, Response Code: {response.status} Error: {text}")
                return (factory, [])
    except asyncio.TimeoutError as e:
        log.error(f"Timeout error for {factory.subscription_id}:{factory}")
        return (factory, [])
    except Exception as e:
        log.error(f"Unable to get url {url} ({factory.subscription_id}:{factory}) due to {e}")
        return (factory, [])
    
async def get_datasets(factory, dataset, headers, session):
    log = logger.getChild(get_datasets.__name__)
    log.debug(f"processing subscription_id:factory:dataset {factory.subscription_id}:{factory.name}:{dataset}")
    url = f"https://management.azure.com/subscriptions/{factory.subscription_id}/resourceGroups/{factory.resource_group}/providers/Microsoft.DataFactory/factories/{factory.name}/datasets/{dataset}?api-version=2018-06-01"
    try:        
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                response_data = await response.json()
                log.info(f"Dataset {factory.subscription_id}:{factory.name}:{dataset} processed successfully.")
                props = response_data.get("properties")
                linked_service_name = props.get("linkedServiceName")
                linked_service_ref = linked_service_name.get("referenceName")
                type_props = props.get("typeProperties")
                props_type = props.get("type")
                if props_type == 'Json':
                    return (factory, dataset, type_props.get("location"), linked_service_ref)
                elif props_type == 'AzureSqlTable':
                    return (factory, dataset, type_props, linked_service_ref)
                else:
                    log.debug(f"Dataset {dataset} type {props_type} not handled")
            else:
                text = await response.text()
                log.error(f"Subscription: {factory.subscription_id}, Response Code: {response.status} Error: {text}")
    except asyncio.TimeoutError as e:
        log.error(f"Timeout error for {factory.subscription_id}:{factory}")
    except Exception as e:
        log.error(f"Unable to get url {url} ({factory.subscription_id}:{factory}) due to {e}")

    return (factory, None, None)

async def get_linkedservices(factory, linked_service, headers, session):
    log = logger.getChild(get_linkedservices.__name__)
    log.debug(f"processing subscription_id:factory:linked_service {factory.subscription_id}:{factory.name}:{linked_service}")
    url = f"https://management.azure.com/subscriptions/{factory.subscription_id}/resourceGroups/{factory.resource_group}/providers/Microsoft.DataFactory/factories/{factory.name}/linkedservices/{linked_service}?api-version=2018-06-01"
    try:        
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                response_data = await response.json()
                log.info(f"LinkedService {factory.subscription_id}:{factory.name}:{linked_service} processed successfully.")
                props = response_data.get("properties")
                props_type = props.get("type")
                type_props = props.get("typeProperties")
                if props_type == 'AzureBlobStorage' or props_type == 'AzureSqlDatabase':
                    return (factory, linked_service, type_props)
                else:
                    log.debug(f"LinkedService {linked_service} type {props_type} not handled")
            else:
                text = await response.text()
                log.error(f"Subscription: {factory.subscription_id}, Response Code: {response.status} Error: {text}")
    except asyncio.TimeoutError as e:
        log.error(f"Timeout error for {factory.subscription_id}:{factory}")
    except Exception as e:
        log.error(f"Unable to get url {url} ({factory.subscription_id}:{factory}) due to {e}")

    return (factory, None, None)
    
async def get(subscription_id, headers, session):
    log = logger.getChild(get.__name__)
    log.debug(f"processing subscription_id {subscription_id}")
    factories = await get_factories(subscription_id, headers, session)

    dataflow_info = await asyncio.gather(*(get_dataflows(factory, dataflow, headers, session) for factory in factories for dataflow in factory.dataflows))
    for (factory, dataset_ref_names) in dataflow_info:
        for dataset_ref_name in dataset_ref_names:
            factory.add_dataset(dataset_ref_name)

    dataset_info = await asyncio.gather(*(get_datasets(factory, dataset, headers, session) for factory in factories for dataset in factory.datasets.keys()))
    for (factory, dataset, info, linked_service_ref) in dataset_info:
        factory.add_dataset_info(dataset, info)
        factory.add_linkservice(linked_service_ref)
    
    linkedservice_info = await asyncio.gather(*(get_linkedservices(factory, linked_service, headers, session) for factory in factories for linked_service in factory.linkservices.keys()))
    for (factory, linkedservice, info) in linkedservice_info:
        factory.add_linkservice_info(linkedservice, info)

    return (subscription_id, factories)

async def main():
    parser = argparse.ArgumentParser(description="Test Image Processing")
    parser.add_argument("--subscription_id", nargs="+", help="Subscription IDs to scan", required=True)
    parser.add_argument("--concurrent", type=int, help="Number of concurrent requests", default=1)
    parser.add_argument('-v', '--verbose', action='count', help="Increase logging level", default=0)
    args = parser.parse_args()

    if args.verbose == 1:
        logger.setLevel(logging.INFO)
    elif args.verbose > 1:
        logger.setLevel(logging.DEBUG)

    default_credential = DefaultAzureCredential()

    headers = { 'Authorization': 'Bearer '+ default_credential.get_token('https://management.azure.com/.default').token }

    conn = aiohttp.TCPConnector(limit=args.concurrent)
    # set total=None because the POST is really slow and the defeault will cause any request still waiting to be processed after "total" seconds to fail.  Also set read to 10 minutes
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=600)

    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        results = await asyncio.gather(*(get(subscription_id, headers, session) for subscription_id in args.subscription_id))

        print(results)

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.WARN)
    asyncio.run(main())