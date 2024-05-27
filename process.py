
import argparse
import asyncio
import logging
import subprocess
import sys
import requests
import json
from azure.identity import DefaultAzureCredential

# datasets and linkedservices

# pipeline -> dataset -> linkservices



def get(url, headers, fields):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = json.loads(response.text)
        val = data.get("value")
        result = []
        for f in fields:
            for v in val:
                result.append(v.get(f))
        return result
    else:
        print(f'Request failed:  {response.status_code} {response.text}')
        return None

# needs: subscription ids to scan, input/output types to record
# val[0]['properties']['activities'][1]['inputs'][0]['type']


async def main():
    parser = argparse.ArgumentParser(description="Test Image Processing")
    parser.add_argument("--subscription_id", nargs="+", help="Subscription IDs to scan", required=True)
    parser.add_argument("--type", nargs="+", help="Types to record", required=True)
    parser.add_argument('-v', '--verbose', action='count', help="Increase logging level", default=0)
    args = parser.parse_args()

    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif args.verbose > 1:
        logging.getLogger().setLevel(logging.DEBUG)

    default_credential = DefaultAzureCredential()

    subscription_id = "7a2964f4-70b7-4541-aa12-7a134b79e105"
    resource_group = "adf-test"

    headers = { 'Authorization': 'Bearer '+ default_credential.get_token('https://management.azure.com/.default').token }

    factory = get(f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.DataFactory/factories?api-version=2018-06-01", headers, ["name"])

    print(factory)
    return

    pipelines = get(f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory[0]}/pipelines?api-version=2018-06-01", headers, "XXX")

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.WARN)
    asyncio.run(main())