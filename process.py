
import argparse
import asyncio
import re
import aiohttp
import logging
import sys
import itertools
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)

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
                result = []
                for v in values:
                    name = v.get("name")
                    id = v.get("id")
                    match = re.search(r"resourceGroups/([^/]+)/providers", id)
                    result.append((name, match.group(1)))
                return result
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
    
async def get_pipelines(subscription_id, resource_group, factory, headers, session):
    log = logger.getChild(get_pipelines.__name__)
    log.debug(f"processing subscription_id:factory {subscription_id}:{factory}")
    url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory}/pipelines?api-version=2018-06-01"
    try:        
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                response_data = await response.json()
                log.info(f"Subscription: {subscription_id} processed successfully.")
                values = response_data.get("value")
                result = []
                for v in values:
                    name = v.get("name")
                    id = v.get("id")
                    match = re.search(r"resourceGroups/([^/]+)/providers", id)
                    result.append((name, match.group(1)))
                return result
            else:
                text = await response.text()
                log.error(f"Subscription: {subscription_id}, Response Code: {response.status} Error: {text}")
                return []
    except asyncio.TimeoutError as e:
        log.error(f"Timeout error for {subscription_id}:{factory}")
        return []
    except Exception as e:
        log.error(f"Unable to get url {url} ({subscription_id}:{factory}) due to {e}")
        return []
    
async def get(subscription_id, headers, session):
    log = logger.getChild(get.__name__)
    log.debug(f"processing subscription_id {subscription_id}")
    factories = await get_factories(subscription_id, headers, session)

    return (subscription_id, factories)

# needs: subscription ids to scan, input/output types to record
# val[0]['properties']['activities'][1]['inputs'][0]['type']


async def main():
    parser = argparse.ArgumentParser(description="Test Image Processing")
    parser.add_argument("--subscription_id", nargs="+", help="Subscription IDs to scan", required=True)
    parser.add_argument("--type", nargs="+", help="Types to record", required=True)
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

    return

    pipelines = get(f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory[0]}/pipelines?api-version=2018-06-01", headers, "XXX")

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.WARN)
    asyncio.run(main())