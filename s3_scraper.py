#!/usr/local/bin/python3

import time
import sys
import xml.etree.ElementTree as etree
import requests

MIN_SIZE = 250000
THROTTLE = 1
MAX_PAGES = 5000

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

otherFiles = []
mp3Files = []
pdfFiles = []
mp3Sizes = []
fileTypes = {}
fileCount = 0
page = 1
errorCount = 0

def writeDiagnostic(lastResponse, finalStateInfo=''):
    diagnosticMsg = f'Pages: {page}, Errors: {errorCount}\n\n==FINAL STATE==\n\n{finalStateInfo}\n\n==FINAL RESPONSE==\n\n{lastResponse}\n\n==HEADERS==\n\n{lastResponse.headers}\n\n==CONTENT==\n\n{lastResponse.content}\n\n==FILE TYPES==\n\n{fileTypes}\n\n=====\n'
    diagnosticFile = open('./diagnostic.out', 'w')
    diagnosticFile.write(diagnosticMsg)
    diagnosticFile.close()

while page <= MAX_PAGES:
    print(f'Fetching page {page}...')

    response = requests.request('GET', initialUrl, params=queryParams)

    if response.status_code != 200:
        errorCount += 1
        print(f'ERROR: Received {response.status_code}\n\n==HEADERS==\n\n{response.headers}\n\n==CONTENT==\n\n{response.content}\n\n=====\n\n')

        # Backoff and retry once
        print('Retrying...\n\n')
        time.sleep(5)
        response = requests.request('GET', initialUrl, params=queryParams)

        if response.status_code != 200:
            print(f'FATAL: Failed on retry. Received {response.status_code}. Aborting.\n\n')
            writeDiagnostic(response, 'Failed on retry.')
            break

    page += 1

    root = etree.fromstring(response.content)
    contentElements = root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}Contents')

    for content in contentElements:
        fileCount += 1
        sizeText = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Size').text
        size = int(sizeText)
        pathText = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key').text

        pathTokens = pathText.split('.')
        if len(pathTokens) > 1:
            fileType = pathTokens[-1]
            fileTypes[fileType] = pathText

        if pathText.endswith(('.mp3', '.mp3.1', '.mp3.2', '.mp3.3')):
            mp3Sizes.append(size)
            if size > MIN_SIZE:
                mp3Files.append((size, pathText))
        elif pathText.endswith('.pdf'):
            pdfFiles.append((size, pathText))
        elif size > MIN_SIZE and pathText.endswith(('.mp4')):
            otherFiles.append((size, pathText))

    print('Done processing.')
    print(f'Count: {len(mp3Files)} target files of {fileCount} total mp3 files.\n')

    # Handle paging
    isTruncatedText = ''
    isTruncatedElements = root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated')
    if isTruncatedElements:
        isTruncatedText = isTruncatedElements[0].text

    continuationTokenElements = root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}NextContinuationToken')

    if continuationTokenElements and isTruncatedText == 'true' and page < MAX_PAGES:
        continuationToken = continuationTokenElements[0].text
        queryParams['continuation-token'] = continuationToken
    else:
        diagnosticMsg = f'continuationToken count: {len(continuationTokenElements)}, isTruncated: {isTruncatedText}'
        writeDiagnostic(response, diagnosticMsg)
        break

    # Throttle otherwise AWS will start to return empty results
    time.sleep(THROTTLE)

mp3FilesSorted = sorted(mp3Files, key=lambda file: file[0], reverse=True)
pdfFilesSorted = sorted(pdfFiles, key=lambda file: file[0], reverse=True)
otherFilesSorted = sorted(otherFiles, key=lambda file: file[0], reverse=True)
print(f'MP3: {mp3FilesSorted[0]}, {mp3FilesSorted[-1]}')
print(f'PDF: {pdfFilesSorted[0]}, {pdfFilesSorted[-1]}')
print(f'OTHER: {otherFilesSorted[0]}, {otherFilesSorted[-1]}')

def writeFile(fileName, dataList, dataTransform):
    data = '\n'.join(map(dataTransform, dataList))
    outFile = open(fileName, 'w')
    outFile.write(data)
    outFile.close()

writeFile('./mp3Sizes.txt', mp3Sizes, str)
writeFile('./mp3Files.txt', mp3FilesSorted, lambda file: f'{file[0]},{file[1]}')
writeFile('./pdfFiles.txt', pdfFilesSorted, lambda file: f'{file[0]},{file[1]}')
writeFile('./otherFiles.txt', otherFilesSorted, lambda file: f'{file[0]},{file[1]}')
