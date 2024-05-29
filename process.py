
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
        self.dataflows = []
        self.datasets = []

    def add_pipeline(self, pipeline):
        self.pipelines.append(pipeline)

    def add_pipelines(self, pipelines):
        self.pipelines.extend(pipelines)

    def add_dataflow(self, dataflow):
        self.dataflows.append(dataflow)

    def add_dataset(self, datasets):
        self.datasets.append(datasets)

    def __str__(self):
        return f"Factory: {self.name} {self.subscription_id} {self.resource_group}"
    
class Pipeline:
    def __init__(self, name):
        self.name = name
        self.activities = []

    def add_activity(self, activity):
        self.activities.append(activity)

    def __str__(self):
        return f"Pipeline: {self.name} {self.pipeline}"


class DataFlowReference:
    def __init__(self, name, reference_name):
        self.name = name
        self.reference_name = reference_name

    def __str__(self):
        return f"DataFlow: {self.name} {self.reference_name}"
    
class DataSetReference:
    def __init__(self, name, reference_name):
        self.name = name
        self.reference_name = reference_name

    def __str__(self):
        return f"DataSet: {self.name} {self.reference_name}"

# pipeline -> dataset -> linkservices
    
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
        return DataFlowReference(factory, activity_name, ref_name)
    elif activity_type == 'Copy':
        inputs = activity.get("inputs")
        outputs = activity.get("outputs")
        for io in itertools.chain(inputs, outputs):
            type = io.get("type")
            return DataSetReference(factory, activity_name, io.get("referenceName"))
    elif activity_type == 'Lookup':
        return None
    else:
        log.debug(f"Activity type {activity_type} not handled")
        return None
    
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
    
async def get(subscription_id, headers, session):
    log = logger.getChild(get.__name__)
    log.debug(f"processing subscription_id {subscription_id}")
    factories = await get_factories(subscription_id, headers, session)

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