#!/usr/local/bin/python3

import time
import sys
from collections import defaultdict
import urllib.request
import mutagen

THROTTLE = 0.5

args = sys.argv

if len(args) < 3:
    print('Usage: downloader.py [S3 bucket] [download paths file]')
    exit()
else:
    bucket = args[1]
    pathsFile = args[2]

baseUrl = f'https://s3.amazonaws.com/{bucket}/'
print(baseUrl)

errors = []
downloadCount = 0

outFile = open("/tmp/headers.out", 'w')

paths = [line.rstrip('\n') for line in open(pathsFile)][:100]

for path in paths:
    downloadCount += 1
    print(str(downloadCount))

    outFile.write("\n==========\n\n")

    url = baseUrl + path
    try:
        fileName, headers = urllib.request.urlretrieve(url)
    except Exception as exception:
        msg = str(exception) + ', ' + str(exception.args) + ', ' + url
        continue

    # TODO: Check header and add to errors
    # if headers.status_code is not 200:
        # errorMsg = "FAIL " + str(headers.status_code) + ": " + path + ", " + fileName + "\n"
        # outFile.write(errorMsg)
        # print(errorMsg)
        # continue

    # TODO: Use ID3 to generate file name and move file
    '''
    > id3 = mutagen.File("/var/folders/_8/m8x38xmd22d_kq1drhcg0nwc0000gn/T/tmpc8qtipmm"
    > type(id3)
    <class 'mutagen.mp3.MP3'>
    > type(id3.tags)
    <class 'mutagen.id3.ID3'>
    > id3.keys()
    dict_keys(['TIT2', 'TPE1', 'TALB', 'TCON', 'TRCK', 'COMM::eng', 'TIT1', 'USLT::eng', 'APIC:', 'TDRC'])
    > id3.tags
    [RAW DUMP]
    > id3.tags["TIT2"]
    TIT2(encoding=<Encoding.UTF16: 1>, text=['Advanced - 小学生'])
    > id3.tags["TIT2"].text[0]
    'Advanced - 小学生'
    > id3.tags["USLT::eng"].text
    [DUMP TRANSCRIPTION]

    http://id3.org/id3v2.3.0#ID3v2_header
    http://id3.org/id3v2.4.0-frames

    TIT2    [#TIT2 Title/songname/content description]
    TPE1    [#TPE1 Lead performer(s)/Soloist(s)]
    TALB    [#TALB Album/Movie/Show title]
    TCON    [#TCON Content type]
    TRCK    [#TRCK Track number/Position in set]
    COMM    [#sec4.11 Comments]
    TIT1    [#TIT1 Content group description]
    USLT    [#sec4.9 Unsychronized lyric/text transcription]
    APIC    [#sec4.15 Attached picture]
    TDRC    Recording time

    1  dict_keys(['TIT2', 'TPE1', 'TALB', 'TCON', 'TRCK', 'COMM::eng',         'USLT::eng', 'APIC:ChinesePod.com',  'TDRC'])
    82 dict_keys(['TIT2', 'TPE1', 'TALB', 'TCON', 'TRCK', 'COMM::eng', 'TIT1', 'USLT::eng', 'APIC:',                'TDRC'])
    17 dict_keys(['TIT2', 'TPE1', 'TALB', 'TCON', 'TRCK', 'COMM::eng', 'TIT1', 'USLT::eng',                         'TDRC'])
    '''
    id3 = mutagen.File(fileName)

    outFile.write(path + "\n" + fileName + "\n" + headers.as_string() + "\n---\n" + str(id3.keys()) + "\n---\n\n")
    outFile.write(str(id3))

    # os.rename(fileName, destination)

    time.sleep(1)

outFile.close




########################################################
'''

# def writeDiagnostic(lastResponse, finalStateInfo=''):
    # fileTypeCountsMsg = ' | '.join([f'{key}: {fileTypeCounts[key]}' for key in fileTypeCounts])
    # diagnosticMsg = f'Pages: {page}, Errors: {errorCount}\n\n==FINAL STATE==\n\n{finalStateInfo}\n\n==FINAL RESPONSE==\n\n{lastResponse}\n\n==HEADERS==\n\n{lastResponse.headers}\n\n==CONTENT==\n\n{lastResponse.content}\n\n==FILE TYPES==\n\nTotal: {len(files)} ({fileTypeCountsMsg})\n\n=====\n'
    # diagnosticFile = open('./diagnostic.out', 'w')
    # diagnosticFile.write(diagnosticMsg)
    # diagnosticFile.close()

# while page <= MAX_PAGES:
    # print(f'Fetching page {page}...')

    # response = requests.request('GET', initialUrl, params=queryParams)

    # if response.status_code != 200:
        # errorCount += 1
        # print(f'ERROR: Received {response.status_code}\n\n==HEADERS==\n\n{response.headers}\n\n==CONTENT==\n\n{response.content}\n\n=====\n\n')

        # # Backoff and retry once
        # print('Retrying...\n\n')
        # time.sleep(5)
        # response = requests.request('GET', initialUrl, params=queryParams)

        # if response.status_code != 200:
            # print(f'FATAL: Failed on retry. Received {response.status_code}. Aborting.\n\n')
            # writeDiagnostic(response, 'Failed on retry.')
            # break

    # page += 1

    # root = etree.fromstring(response.content) # contentElements = root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}Contents')

    # for content in contentElements:
        # sizeText = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Size').text
        # size = int(sizeText)
        # pathText = content.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key').text

        # fileType = 'NONE'
        # pathTokens = pathText.split('.')
        # if len(pathTokens) > 1:
            # fileType = pathTokens[-1]

            # if fileType in ['1', '2', '3', '4', '5', '6', '7', '8', '9', 'old'] and len(pathTokens) > 2:
                # fileType = pathTokens[-2]

            # fileTypeCounts[fileType] += 1

            # if fileType == 'mp3' and size > MIN_SIZE:
                # fileTypeCounts['mp3+'] += 1

        # files.append((size, fileType, pathText))

    # print('Done processing page.')
    # fileTypeCountsMsg = ' | '.join([f'{key}: {fileTypeCounts[key]}' for key in fileTypeCounts])
    # print(f'File count: {len(files)} ({fileTypeCountsMsg})')

    # # Handle paging
    # isTruncatedText = ''
    # isTruncatedElements = root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated')
    # if isTruncatedElements:
        # isTruncatedText = isTruncatedElements[0].text

    # continuationTokenElements = root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}NextContinuationToken')

    # if continuationTokenElements and isTruncatedText == 'true' and page < MAX_PAGES:
        # continuationToken = continuationTokenElements[0].text
        # queryParams['continuation-token'] = continuationToken
    # else:
        # diagnosticMsg = f'continuationToken count: {len(continuationTokenElements)}, isTruncated: {isTruncatedText}'
        # writeDiagnostic(response, diagnosticMsg)
        # break

    # # Throttle otherwise AWS will start to return empty results
    # time.sleep(THROTTLE)

# filesSorted = sorted(files, key=lambda file: file[0], reverse=True)
# print(f'Largest file: {filesSorted[0]}')

# def writeFile(fileName, dataList, dataTransform):
    # data = '\n'.join(map(dataTransform, dataList))
    # outFile = open(fileName, 'w')
    # outFile.write(data)
    # outFile.close()

# writeFile('./files.txt', filesSorted, str)
# writeFile('./files.txt', filesSorted, lambda file: f'{file[0]},{file[1]},{file[2]}')
'''
