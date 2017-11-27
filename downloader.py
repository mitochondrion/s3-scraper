#!/usr/local/bin/python3

import time
import sys
import os
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
destinationPath = '/Users/steakknife/Documents/cpod/'
errors = []
outFile = open("/tmp/downloader.out", 'w')
startIndex = 0
untilIndex = 99999
downloadCount = startIndex

paths = [line.rstrip('\n') for line in open(pathsFile)][startIndex:untilIndex]

for path in paths:
    downloadCount += 1
    print(str(downloadCount))
    url = baseUrl + path

    try:
        fileName, headers = urllib.request.urlretrieve(url)
    except Exception as exception:
        errorMsg = 'FAIL(urllib) ' + str(downloadCount) + ': ' + url + '\nDESC: ' + str(exception) + '\nARGS: ' + str(exception.args) + '\n\n'
        print(errorMsg)
        outFile.write(errorMsg)
        continue

    id3 = mutagen.File(fileName)

    try:
        trackNumber = id3.tags['TRCK'].text[0]
        trackAlbum = id3.tags['TALB'].text[0].replace('/', '|')
        trackTitle = id3.tags['TIT2'].text[0].replace('/', '|')
    except Exception as exception:
        errorMsg = 'FAIL(id3) ' + str(downloadCount) + ': ' + str(exception) + '\nFILE: ' + fileName + '\n\n'
        print(errorMsg)
        outFile.write(errorMsg)
        continue

    try:
        destinationFileName = trackNumber + ' - ' + trackTitle + ' - ' + trackAlbum + '.mp3'
        destination = destinationPath + destinationFileName
        os.rename(fileName, destination)
    except Exception as exception:
        errorMsg = 'FAIL(os) ' + str(downloadCount) + ': ' + str(exception) + '\nFILE: ' + fileName + '\n\n'
        print(errorMsg)
        outFile.write(errorMsg)
        break

    time.sleep(3)

outFile.close



##################################################
'''
    'TIT2'
    ['Intermediate - Gift for a New Girlfriend?']
    'TPE1'
    ['ChinesePod.com']
    'TALB'
    ['show']
    'TCON'
    ['Podcast']
    'TRCK'
    ['3099']
    'COMM::eng'
    ['Learn Chinese with ChinesePod. Get professional podcasts and advanced online tools to help you learn Chinese, and connect with thousands of other Chinese learners worldwide.']
    'TIT1'
    ['Intermediate']
    'USLT::eng'
    Dialogue
    'TDRC'
    ['2017']

========================

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
