#!/usr/local/bin/python3

import xml.etree.ElementTree as etree
import requests
import time
import sys

MIN_SIZE = 500000
THROTTLE = 1
MAX_PAGES = 1500

args = sys.argv

if len(args) < 2:
    print('Missing bucket argument!')
    exit()
else:
    bucket = args[1]

initialUrl = f'https://s3.amazonaws.com/{bucket}/'
print(initialUrl)

queryParams = {
    'list-type': '2',
    'max-keys': '1000'
}

targetFiles = []
sizes = []
page = 1

while page <= MAX_PAGES:
    print(f'Fetching page {page}...')

    response = requests.request('GET', initialUrl, params=queryParams)

    if response.status_code != 200:
        print(f'ERROR: Received {response.status_code}\n\n==HEADERS==\n\n{response.headers}\n\n==CONTENT==\n\n{response.content}\n\n=====\n\n')

        # Backoff and retry once
        print('Retrying...\n\n')
        time.sleep(5)
        response = requests.request('GET', initialUrl, params=queryParams)

        if response.status_code != 200:
            print(f'FATAL: Received {response.status_code}\n\n==HEADERS==\n\n{response.headers}\n\n==CONTENT==\n\n{response.content}\n\n=====\n\n')
            print('Failed on retry. Aborting.\n\n')
            break

    page += 1

    root = etree.fromstring(response.content)
    contentElements = root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}Contents')

    for content in contentElements:
        sizeText = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Size').text
        size = int(sizeText)
        sizes.append(size)
        path = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key')

        if size > MIN_SIZE and path is not None:
            pathText = path.text
            if pathText.endswith('.mp3'):
                targetFiles.append((size, pathText))

    print('Done processing.')
    print(f'Count: {len(targetFiles)} target files of {len(sizes)} total files.\n')

    # Handle paging

    isTruncatedText = ''
    isTruncatedElements =  root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated')
    if isTruncatedElements:
        isTruncatedText = isTruncatedElements[0].text

    continuationTokenElements = root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}NextContinuationToken')

    if continuationTokenElements and isTruncatedText == 'true' and page < MAX_PAGES:
        continuationToken = continuationTokenElements[0].text
        queryParams['continuation-token'] = continuationToken
    else:
        diagnosticMsg = f'==FINAL STATE==\n\ncontinuationToken count: {len(continuationTokenElements)}, isTruncated: {isTruncatedText}\n\n==FINAL RESPONS==\n\n{response}\n\n==HEADERS==\n\n{response.headers}\n\n==CONTENT==\n\n{response.content}\n\n==XML==\n\n{etree.tostring(root, encoding="utf8", method="xml")}\n\n=====\n'
        diagnosticFile = open('./diagnostic.out', 'w')
        diagnosticFile.write(diagnosticMsg)
        diagnosticFile.close()
        break

    # Throttle otherwise AWS will start to return empty results
    time.sleep(THROTTLE)

targetFilesSorted = sorted(targetFiles, key=lambda file: file[0], reverse=True)
print(f'{targetFilesSorted[0]}, {targetFilesSorted[len(targetFilesSorted)-1]}')

sizesData = '\n'.join(
    map(
        lambda size: str(size),
        sizes
    )
)
sizesFile = open('./sizes.txt', 'w')
sizesFile.write(sizesData)
sizesFile.close()

filesData = '\n'.join(
    map(
        lambda file: f'{file[0]},{file[1]}\n',
        targetFilesSorted
    )
)
filesFile = open('./files.txt', 'w')
filesFile.write(filesData) 
filesFile.close()
