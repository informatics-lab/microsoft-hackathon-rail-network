# Databricks notebook source
# MAGIC %pip install eccodes==1.2.0
# MAGIC %pip install ecmwflibs
# MAGIC %pip install xarray==2022.3.0
# MAGIC %pip install cfgrib==0.9.10.1
# MAGIC %pip install azure-storage-blob

# COMMAND ----------

# 2021 (C) Crown Copyright, Met Office. All rights reserved.
#
# This file is part of Weather DataHub and is released under the
# BSD 3-Clause license.
# See LICENSE in the root of the repository for full licensing details.
# (c) Met Office 2022

# See https://github.com/MetOffice/weather_datahub_utilities/blob/main/atmospheric_order_download/Documentation.md

import csv, os
import requests
import argparse
import time
from datetime import datetime
import queue
import threading
import uuid

# Example code to download GRIB data files from the Met Office Weather DataHub via API calls

MODEL_LIST = ["mo-global", "mo-uk", "mo-uk-latlon", "mo-mogrepsg"]
BASE_URL = "https://api-metoffice.apiconnect.ibmcloud.com/metoffice/production/1.0.0"
debugMode = False
printUrl = False
retryCount = 3


def get_order_details(
    baseUrl, requestHeaders, orderName, useEnhancedApi, runsToDownload
):

    details = None

    actualHeaders = {"Accept": "application/json"}
    actualHeaders.update(requestHeaders)

    url = baseUrl + "/orders/" + orderName + "/latest"
    if useEnhancedApi:
        url = url + "?detail=MINIMAL"
        if len(runsToDownload) == 1:
            url = url + "&runfilter=" + runsToDownload[0]

    req = requests.get(url, headers=actualHeaders)

    if verbose and apikey == "":
        print("Plan and limit : " + req.headers["X-RateLimit-Limit"])
        print("Remaining calls: " + req.headers["X-RateLimit-Remaining"])

    if printUrl == True:
        print("get_order_details: ", url)
        if url != req.url:
            print("redirected to: ", req.url)

    if req.status_code != 200:
        print(
            "ERROR: Unable to load details for order : ",
            orderName,
            " status code: ",
            req.status_code,
        )
        exit()
    else:
        details = req.json()

    return details


def get_order_file(
    baseUrl, requestHeaders, orderName, fileId, guidFileNames, folder, start
):

    # If file id is too long or random file names required generate a uuid for the file name

    urlMod = ""
    global debugMode

    if len(fileId) > 100 or guidFileNames:
        local_filename = folder + "/" + str(uuid.uuid4()) + ".grib2"
    else:
        local_filename = folder + "/" + fileId + ".grib2"

    ttfb = 0

    url = baseUrl + "/orders/" + orderName + "/latest/" + fileId + "/data"

    if debugMode == True:
        urlMod = input(
            "Order: "
            + orderName
            + " File:"
            + fileId
            + "\n"
            + "Enter y to mimic a receive failure on file - 'go' to run to end> "
        )
        # If you put go all further runs will automatically go through
        if urlMod == "go":
            debugMode = False
        if debugMode == True and urlMod == "y":
            url = (
                baseUrl
                + "/orders/"
                + orderName
                + "/latest/"
                + fileId
                + urlMod
                + "/data"
            )

    actualHeaders = {"Accept": "application/x-grib"}
    actualHeaders.update(requestHeaders)

    with requests.get(
        url, headers=actualHeaders, allow_redirects=True, stream=True
    ) as r:

        if printUrl == True:
            print("get_order_file: ", url)
            if url != r.url:
                print("redirected to: ", r.url)

        if r.status_code != 200:

            raise Exception("HTTP Reason and Status: " + r.reason, r.status_code)

        # Record time to first byte
        ttfb = start + r.elapsed.total_seconds()

        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return [ttfb, local_filename]


def get_files_by_run(order, runsToDownload, numFilesPerOrder):

    # Break down the files in to those needed for each run
    filesByRun = {}
    for run in runsToDownload:

        filesByRun[run] = []
        fc = 0
        for f in order["orderDetails"]["files"]:
            fileId = f["fileId"]
            if "_+" + run in fileId:
                filesByRun[run].append(fileId)
                fc += 1
                if numFilesPerOrder > 0 and fc >= numFilesPerOrder:
                    break

    return filesByRun


def download_worker():

    if taskQueue:

        while True:
            downloadTask = taskQueue.get()
            if downloadTask is None:

                break

            current_time = datetime.now().strftime("%H-%M-%S-%f")

            fileSize = 0
            errMsg = ""
            error = False
            timeToFirstByte = 0
            startTime = time.time()
            try:
                downloadResp = get_order_file(
                    downloadTask["baseUrl"],
                    downloadTask["requestHeaders"],
                    downloadTask["orderName"],
                    downloadTask["fileId"],
                    downloadTask["guidFileNames"],
                    downloadTask["folder"],
                    startTime,
                )
                timeToFirstByte = round((downloadResp[0] - startTime), 2)
                downloadedFile = downloadResp[1]
                fileSize = os.path.getsize(downloadedFile)

            except Exception as ex:
                error = True
                errMsg = ex.args

            completeTime = time.time()
            completeDuration = round((completeTime - startTime), 2)

            if error:
                downloadTask["downloadErrorLog"].append(
                    {
                        "URL": downloadTask["baseUrl"]
                        + "/orders/"
                        + downloadTask["orderName"]
                        + "/latest/"
                        + downloadTask["fileId"]
                        + "/data",
                        "fileid": downloadTask["fileId"],
                        "currentTime": current_time,
                        "ordername": downloadTask["orderName"],
                        "folder": downloadTask["folder"],
                    }
                )
                downloadTask["responseLog"].append(
                    {
                        "order": downloadTask["orderName"],
                        "fileId": downloadTask["fileId"],
                        "error": error,
                        "fileSize": fileSize,
                        "errMsg": errMsg,
                        "time_to_first_byte": timeToFirstByte,
                        "duration": completeDuration,
                        "file": "",
                        "currentTime": current_time,
                    }
                )
                if verbose:
                    print(
                        "File: "
                        + downloadTask["fileId"]
                        + " failed "
                        + format(errMsg)
                        + "\n"
                    )
            else:
                downloadTask["responseLog"].append(
                    {
                        "order": downloadTask["orderName"],
                        "fileId": downloadTask["fileId"],
                        "error": error,
                        "fileSize": fileSize,
                        "errMsg": errMsg,
                        "time_to_first_byte": timeToFirstByte,
                        "duration": completeDuration,
                        "file": downloadedFile,
                        "currentTime": current_time,
                    }
                )

            taskQueue.task_done()


def write_failures(downloadErrorLog, fileName):

    if len(downloadErrorLog) == 0:
        return

    with open(fileName, "w") as failurefile:

        for line in downloadErrorLog:
            failurefile.write(line["URL"] + "\n")

    failurefile.close()


def write_summary(responseLog, fileName, sstartTime):

    endTime = datetime.now()

    if len(responseLog) == 0:
        return

    with open(fileName, "w", newline="") as csvfile:

        fileSizeTotal = 0
        index = 0

        csvfile.write(
            "The download of order ["
            + responseLog[0]["order"]
            + "] started at: "
            + sstartTime.strftime("%d/%m/%Y %H:%M:%S")
            + " finished at: "
            + endTime.strftime("%d/%m/%Y %H:%M:%S\n")
        )

        for row in responseLog:
            fileSizeTotal += responseLog[index]["fileSize"]
            index += 1
        csvfile.write(
            "Total Files: "
            + str(len(responseLog))
            + " Total time taken: "
            + str(round((endTime - sstartTime).total_seconds(), 2))
            + "s Total Size: "
            + str(fileSizeTotal)
            + " Workers: "
            + str(numThreads)
            + "\n"
        )
        csvfile.write("===== Detail Section =====\n")

        fieldnames = [
            "order",
            "duration",
            "time_to_first_byte",
            "fileSize",
            "fileId",
            "error",
            "errMsg",
            "file",
            "currentTime",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in responseLog:
            writer.writerow(row)
        # csvfile.write("Total Files: " + str(len(responseLog)) +  " Total time taken: " + str(round((endTime-sstartTime).total_seconds(),2)) + "s Total Size: " + str(fileSizeTotal) + "\n")
        if verbose:
            print(
                "    Total Files: "
                + str(len(responseLog))
                + " Total time taken: "
                + str(round((endTime - sstartTime).total_seconds(), 2))
                + "s Total Size: "
                + str(fileSizeTotal)
                + " Workers: "
                + str(numThreads)
                + "\n"
            )


def join_files(responseLog, joinedFileName):

    with open(joinedFileName, "wb") as outfile:
        for f in responseLog:

            with open(f["file"], "rb") as infile:
                while True:
                    chunk = infile.read(8192)
                    if not chunk:
                        break
                    outfile.write(chunk)
            os.remove(f["file"])


def get_my_orders(baseUrl, requestHeaders):

    ordHeaders = {"Accept": "application/json"}
    ordHeaders.update(requestHeaders)

    ordurl = baseUrl + "/orders?detail=MINIMAL"
    ordr = requests.get(ordurl, headers=ordHeaders)
    if printUrl == True:
        print("get_my_orders: ", ordurl)
        if ordurl != ordr.url:
            print("redirected to: ", ordr.url)

    if ordr.status_code != 200:
        print("ERROR:  Unable to get my orders list. Status code: ", ordr.status_code)
        exit()
    orddetails = ordr.json()

    return orddetails


def get_latest_run(modelID, orderName, modelRuns):

    latestRun = modelRuns[modelID][:2]
    latestDate = modelRuns[modelID][3:]
    stamp = latestDate[:10] + ":" + latestRun
    if not os.path.exists(baseFolder + LATEST_FOLDER + "/" + orderName + ".txt"):
        # File not there - so write it and return latest run
        rf = open(baseFolder + LATEST_FOLDER + "/" + orderName + ".txt", "w")
        rf.write(stamp)
        rf.close()
    else:
        # Open the file and retrieve the last run
        rf = open(baseFolder + LATEST_FOLDER + "/" + orderName + ".txt", "r")
        laststamp = rf.read()
        rf.close()
        # Check to see if the latest is later than the last run
        if stamp > laststamp:
            rf = open(baseFolder + LATEST_FOLDER + "/" + orderName + ".txt", "w")
            rf.write(stamp)
            rf.close()
        else:
            latestRun = "done" + ":" + latestRun

    return latestRun


def get_model_runs(baseUrl, requestHeaders, modelList):

    modelRuns = {}
    runHeaders = {"Accept": "application/json"}
    runHeaders.update(requestHeaders)

    for model in modelList:
        requrl = baseUrl + "/runs/" + model + "?sort=RUNDATETIME"

        for loop in range(retryCount):

            reqr = requests.get(requrl, headers=runHeaders)

            if printUrl == True:
                print("get_model_runs: ", requrl)
                if requrl != reqr.url:
                    print("redirected to: ", reqr.url)

            if reqr.status_code != 200:
                print(
                    "ERROR:  Unable to get latest run for model: "
                    + model
                    + " status code: ",
                    reqr.status_code,
                )
                if loop != (retryCount - 1):
                    time.sleep(10)
                    continue
                else:
                    print("ERROR:  Ran out of retries to get latest run for model: ")
                    break

            rundetails = reqr.json()
            rawlatest = rundetails["completeRuns"]
            modelRuns[model] = rawlatest[0]["run"] + ":" + rawlatest[0]["runDateTime"]
            break

    return modelRuns


def run_wanted(allorders, ordername, latestrun):

    result = False
    for ords in allorders["orders"]:
        if ords["orderId"] == ordername:
            if latestrun in ords["requiredLatestRuns"]:
                result = True
            else:
                result = False

    return result


def order_exists(allorders, ordername):

    result = False
    for ords in allorders["orders"]:
        if ords["orderId"] == ordername:
            result = True
            break

    return result


def get_model_from_order(allorders, ordername):

    result = "Not found"
    for ords in allorders["orders"]:
        if ords["orderId"] == ordername:
            result = ords["modelId"]
            break

    return result




ROOT_FOLDER = "downloaded"
LATEST_FOLDER = "latest"
RESULTS_FOLDER = "results"
FAILURES_FOLDER = "failures"
baseUrl = ""
clientId = ""
secret = ""
orderRuns = ""
useEnhancedApi = ""
verbose = ""
folderdate = ""
numThreads = ""
myModelList = ""
joinFiles = ""
retry = ""
retryperiod = ""
debugMode = ""
baseFolder = ""
apikey = ""
printUrl = ""
taskQueue = None


def download_from_weatherdatahub():
    parser = argparse.ArgumentParser(
        description="Download all the files for one or more order from the CDA delivery service."
    )
    parser.add_argument(
        "-u",
        "--url",
        action="store",
        dest="baseUrl",
        default=BASE_URL,
        help="Base URL used to access Weather DataHub API. Defaults to https://api-metoffice.apiconnect.ibmcloud.com/metoffice/production/1.0.0.",
    )
    parser.add_argument(
        "-c",
        "--client",
        action="store",
        dest="clientId",
        default="",
        help="REQUIRED: Client ID of your WDH Application",
    )
    parser.add_argument(
        "-s",
        "--secret",
        action="store",
        dest="secret",
        default="",
        help="REQUIRED: Your WDH API Gateway secret",
    )
    parser.add_argument(
        "-o",
        "--orders",
        action="store",
        dest="ordersToDownload",
        default="default_order",
        help="REQUIRED: Comma separated list of order names to download.",
    )
    parser.add_argument(
        "-r",
        "--runs",
        action="store",
        dest="orderRuns",
        default="00,06,12,18",
        help="Comma separated list of runs to download or -r latest to get latest run.",
    )
    parser.add_argument(
        "-w",
        "--workers",
        action="store",
        dest="workers",
        default=4,
        type=int,
        help="Number of workers used to perform downloads. Defaults to 4.",
    )
    parser.add_argument(
        "-j",
        "--join",
        action="store_true",
        dest="joinFiles",
        default=False,
        help="If present, all the downloaded files will be concatenated together.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="Verbose mode.",
    )
    parser.add_argument(
        "-d",
        "--folderdate",
        action="store_true",
        dest="folderdate",
        default=False,
        help="Add the YYYYMMDDhhmm_RR to the download folder.",
    )
    parser.add_argument(
        "-l",
        "--location",
        action="store",
        dest="location",
        default="",
        help="The base folder to store files",
    )
    parser.add_argument(
        "-m",
        "--modellist",
        action="store",
        dest="modellist",
        default=MODEL_LIST,
        help="Pass the ist of models to support.",
    )
    parser.add_argument(
        "-a",
        "--retry",
        action="store_true",
        dest="retry",
        default=False,
        help="Retry again the failures automatically.",
    )
    parser.add_argument(
        "-p",
        "--retryperiod",
        action="store",
        dest="retryperiod",
        default="300",
        help="Retry delay in seconds.",
    )
    parser.add_argument(
        "-x",
        "--printurl",
        action="store_true",
        dest="printurl",
        default=False,
        help="Print all accessed URLs and redirects",
    )
    parser.add_argument(
        "-z",
        "--debug",
        action="store_true",
        dest="debugmode",
        default=False,
        help="Switch to debug mode.",
    )
    parser.add_argument(
        "-k",
        "--apikey",
        action="store",
        dest="apikey",
        default="",
        help="Use direct API Key when not via APIM.",
    )

    args = parser.parse_args()

    global baseUrl
    global clientId
    global secret
    global orderRuns
    global useEnhancedApi
    global verbose
    global folderdate
    global numThreads
    global myModelList
    global joinFiles
    global retry
    global retryperiod
    global debugMode
    global baseFolder
    global apikey
    global printUrl

    baseUrl = args.baseUrl
    clientId = args.clientId
    secret = args.secret
    orderRuns = args.orderRuns
    useEnhancedApi = True
    verbose = args.verbose
    folderdate = args.folderdate
    numThreads = args.workers
    myModelList = args.modellist
    joinFiles = args.joinFiles
    retry = args.retry
    retryperiod = args.retryperiod
    debugMode = args.debugmode
    baseFolder = args.location
    apikey = args.apikey
    printUrl = args.printurl

    if debugMode == True:
        print("WARNING: As we are in debug mode setting workers to one.")
        numThreads = 1

    if args.ordersToDownload == "":
        print("ERROR: You must pass an orders list to download.")
        exit()
    else:
        ordersToDownload = args.ordersToDownload.lower().split(",")

    joinFiles = False
    numFilesPerOrder = 0
    guidFileNames = False

    # Client ID and Sectet must be supplied
    if (clientId == "" or secret == "") and apikey == "":
        print("ERROR: IBM client and secret must be supplied.")
        exit()

    if apikey == "":
        requestHeaders = {"x-ibm-client-id": clientId, "x-ibm-client-secret": secret}
    else:
        requestHeaders = {"x-api-key": apikey}

    if baseFolder != "":
        try:
            if baseFolder[-1] != "/":
                baseFolder = baseFolder + "/"
            os.makedirs(baseFolder, exist_ok=True)
        except OSError as error:
            print("ERROR: Base folder", baseFolder, "cannot be accessed or created.")
            exit()

    os.makedirs(baseFolder + ROOT_FOLDER, exist_ok=True)
    os.makedirs(baseFolder + LATEST_FOLDER, exist_ok=True)
    os.makedirs(baseFolder + RESULTS_FOLDER, exist_ok=True)
    os.makedirs(baseFolder + FAILURES_FOLDER, exist_ok=True)

    if verbose:
        print("Download Orders")
        print("===============")

    ordersfound = False

    # Get my orders for future reference
    myOrders = get_my_orders(baseUrl, requestHeaders)

    if len(myOrders["orders"]) == 0:
        print(
            "WARNING: You have no orders active on Weather DataHub.  Please confirm some orders and try again."
        )
        exit()

    # For each of the orders to download get the model and add to my model list
    myModelList = []
    for orderName in ordersToDownload:
        newModel = get_model_from_order(myOrders, orderName)
        if newModel not in myModelList:
            myModelList.append(newModel)
    if verbose == True:
        print(
            "From the orders to process we have the following model list from active orders: ",
            myModelList,
        )

    if myModelList == [] or myModelList == ["Not found"]:
        print(
            "ERROR: No models could be extracted from the orders to process: "
            + str(ordersToDownload)
        )
        exit()

    myModelRuns = get_model_runs(baseUrl, requestHeaders, myModelList)

    retryManifest = []

    # Total number of files downloaded

    totalFiles = 0
    finalRuns = []
    myTimeStamp = datetime.now().strftime("%d-%b-%Y-%H-%M-%S")

    # Process selected orders, generating tasks for the worker to actually download the file.
    for orderName in ordersToDownload:

        initTime = datetime.now()

        responseLog = []
        downloadErrorLog = []
        if verbose:
            print("Processing: " + orderName)
        if not order_exists(myOrders, orderName):
            print(
                "ERROR: You've asked for an order called: "
                + orderName
                + " which doesn't appear in the list of active orders."
            )
            continue
        if orderRuns == "":
            runsToDownload = ["00", "06", "12", "18"]
        else:
            if orderRuns == "latest":
                modelToGet = get_model_from_order(myOrders, orderName)
                if modelToGet not in myModelList:
                    print(
                        "ERROR: No idea what model: "
                        + modelToGet
                        + " is so terminating!"
                    )
                    exit()
                runsToDownload = get_latest_run(modelToGet, orderName, myModelRuns)
                if runsToDownload[:4] == "done":
                    if verbose:
                        print(
                            "We have done this latest run "
                            + runsToDownload[5:]
                            + " already!"
                        )
                    continue
                # Do I want this run?
                finalRuns = []
                runWanted = run_wanted(myOrders, orderName, runsToDownload)
                if runWanted and verbose:
                    print("This run " + runsToDownload + " is wanted.")
                else:
                    if verbose:
                        print("This run " + runsToDownload + " is not wanted")
                    continue
                runsToDownload = runsToDownload.split(",")
                finalRuns.append(runsToDownload)

            else:
                runsToDownload = orderRuns.split(",")
                finalRuns = []
                # Ensure only runs wanted are asked for
                for checkRun in runsToDownload:
                    if run_wanted(myOrders, orderName, checkRun):
                        finalRuns.append(checkRun)
                    else:
                        print(
                            "WARNING: The run "
                            + checkRun
                            + " has been asked for but doesn't appear in the order "
                            + orderName
                        )
                runsToDownload = finalRuns

        if len(finalRuns) == 0:
            print(
                "WARNING: No runs for order "
                + orderName
                + "were found.  Don't expect any data."
            )
            continue

        order = get_order_details(
            baseUrl, requestHeaders, orderName, useEnhancedApi, runsToDownload
        )
        if order != None:
            global taskQueue
            # Create queue and threads for processing downloads
            taskQueue = queue.Queue()
            taskThreads = []
            for i in range(numThreads):
                t = threading.Thread(target=download_worker)
                taskThreads.append(t)
            # End of set up threads
            ordersfound = True

            # Break down the files in to those needed for each run
            filesByRun = get_files_by_run(order, runsToDownload, numFilesPerOrder)

            # Now queue up tasks to down load each file
            for run in runsToDownload:

                if folderdate == True:
                    folder = (
                        baseFolder
                        + ROOT_FOLDER
                        + "/"
                        + initTime.strftime("%Y%m%d%H%M_")
                        + run
                        + "/"
                        + orderName
                        + "_"
                        + run
                    )
                else:
                    folder = baseFolder + ROOT_FOLDER + "/" + orderName + "_" + run

                os.makedirs(folder, exist_ok=True)
                for fileId in filesByRun[run]:
                    downloadTask = {
                        "baseUrl": baseUrl,
                        "requestHeaders": requestHeaders,
                        "orderName": orderName,
                        "fileId": fileId,
                        "guidFileNames": guidFileNames,
                        "folder": folder,
                        "responseLog": responseLog,
                        "downloadErrorLog": downloadErrorLog,
                    }
                    taskQueue.put(downloadTask)

        # Start the worker threads
        if ordersfound == False:
            print(
                "WARNING: No orders or runs were found from this list: ",
                ordersToDownload,
            )
            continue

        if verbose:
            print("    Starting downloads")
        for t in taskThreads:
            t.start()

        # Wait for all the queued scenarios to be processed
        taskQueue.join()

        # Stop all the threads
        for i in range(numThreads):
            taskQueue.put(None)

        for t in taskThreads:
            t.join()

        # Write out the summary CSV file
        summaryFileName = (
            baseFolder + "results/summary-" + orderName + "-" + myTimeStamp + ".txt"
        )
        failuresFileName = (
            baseFolder + "failures/summary-" + orderName + "-" + myTimeStamp + ".txt"
        )

        if len(downloadErrorLog) > 0:
            write_failures(downloadErrorLog, failuresFileName)
            print(
                "WARNING: there were",
                len(downloadErrorLog),
                "detected download failures\nDetails in file: " + failuresFileName,
            )
            if retry:
                retryManifest = retryManifest + downloadErrorLog

        write_summary(responseLog, summaryFileName, initTime)
        totalFiles = totalFiles + len(responseLog)

        if verbose and len(responseLog) > 0:
            print("    Created summary: " + summaryFileName)

    # End of order processing loop

    if verbose:
        print("All file downloads have been attempted.")

    # Do we have any retries we want to do
    if retry and len(retryManifest) > 0:
        if verbose:
            print("We have files to retry")
        totalFailures = len(retryManifest)
        failureRate = (totalFailures / totalFiles) * 100.00
        if verbose:
            print("The failure rate is", failureRate, "percent.")

        if totalFailures > 100:
            print(
                "ERROR: total failures of",
                totalFailures,
                "is more than the 100 limit can't recover.",
            )
            exit()

        if totalFailures == totalFiles:
            print(
                "ERROR: Everything failed for all",
                totalFiles,
                "files - terminating program.",
            )
            exit()

        if failureRate > 50.0 and totalFailures > 50:
            print(
                "ERROR: failure rate > 50 percent and more than 20 failures - terminating."
            )
            exit()

        # I can now retry
        # Wait for the asked time
        if verbose:
            print("Wait of", retryperiod, "starting.")
        time.sleep(int(retryperiod))
        if verbose:
            print("Wait of", retryperiod, "ended.")
        # Wait ended

        actualHeaders = {"Accept": "application/x-grib"}
        actualHeaders.update(requestHeaders)
        stillInError = []
        deleteFile = False

        for retryFile in retryManifest:
            if verbose:
                print("Re-trying " + retryFile["fileid"])
            startTime = time.time()
            failuresFileName = (
                baseFolder
                + "failures/summary-"
                + orderName
                + "-"
                + myTimeStamp
                + ".txt"
            )
            summaryFileName = (
                baseFolder + "results/summary-" + orderName + "-" + myTimeStamp + ".txt"
            )
            if deleteFile == False:
                if os.path.isfile(failuresFileName):
                    os.remove(failuresFileName)
                deleteFile = True

            error = False

            try:
                if apikey != "":
                    requestHeaders = {"x-api-key": apikey}

                if verbose:
                    print(
                        "Retrying",
                        baseUrl,
                        retryFile["ordername"],
                        retryFile["fileid"],
                        retryFile["folder"],
                    )
                downloadResp = get_order_file(
                    baseUrl,
                    requestHeaders,
                    retryFile["ordername"],
                    retryFile["fileid"],
                    False,
                    retryFile["folder"],
                    startTime,
                )
                fileSize = os.path.getsize(downloadResp[1])

            except Exception as ex:
                error = True
                errMsg = ex.args
                status = ex.args[1]

            if not error:
                with open(summaryFileName, "a") as sumfile:
                    sumfile.write(
                        retryFile["ordername"]
                        + ",0,0,0,"
                        + retryFile["fileid"]
                        + ",False,RETRY-OK,"
                        + downloadResp[1]
                        + ","
                        + datetime.now().strftime("%H-%M-%S-%f")
                        + "\n"
                    )
                sumfile.close()

            else:
                with open(failuresFileName, "a") as errfile:
                    errfile.write(
                        "File "
                        + retryFile["fileid"]
                        + " FAILED on retry. errMsg: "
                        + format(errMsg)
                        + " status: "
                        + str(status)
                        + "\n"
                    )
                errfile.close()
                stillInError.append(retryFile.copy())


# COMMAND ----------

from math import floor
import os
import sys
from datetime import datetime
from glob import glob
import xarray as xr
from azure.storage.blob import BlobServiceClient
import cfgrib
DOWNLOAD_FOLDER = "./test_data"  # TODO: move to tmp?
ORDER_NUMBER = 'o111040072014'  # TODO: update order no (the O should be lower case!)


def main():
    latest_run = get_latest_mogreps_run()
    print(f"Fetching run {latest_run}")
    set_arguments(latest_run)
    fetch_data()

    connect_str = "DefaultEndpointsProtocol=https;AccountName=moensembledata;AccountKey=DG1JH+DzSNLxI4kKKPlu1wwOSXSopn69sMU0nYqbFptqJsNs8x3txu+DNACKoJUBskLKP/Lwt5a8+AStT2GnhA==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    for parameter_name in [
        "agl_temperature", 
#         "wind-direction-from-which-blowing-surface-adjusted",
#         "wind-speed-gust",
#         "wind-speed-surface-adjusted",
#         "soil-moisture_0.05",
#         "soil-moisture_0.225",
#         "soil-moisture_0.675",
#         "soil-moisture_2.0",
#         "convective-rain-accumulation",
#         "downward-short-wave-radiation-flux",
#         "rainfall-accumulation",
#         "ground_temperature",
#         "pressure-reduced-to-msl",
    ]:
        out_filepath = convert_and_save_netcdf_xr(
            f"test_data/downloaded/{ORDER_NUMBER}_{latest_run}", parameter_name
        )
        copy_to_blob(blob_service_client, out_filepath)


def copy_to_blob(blob_service_client, filepath):
    out_filename = f"{datetime.now().year}_{datetime.now().month}_{datetime.now().date}_{get_latest_mogreps_run()}_{os.path.split(filepath)[1]}"
    blob_client = blob_service_client.get_blob_client(
        container="weatherdatahuboutput",
        blob=out_filename,
    )
    with open(filepath, "rb") as data:
        blob_client.upload_blob(data)


def set_arguments(latest_run):
    sys.argv = [
        'main',
        '-c', '',  # TODO: get from Azure keyvault
        '-s', '',  # TODO: get from Azure keyvault
        '-o', ORDER_NUMBER,
        '-l', DOWNLOAD_FOLDER,
        '-r', latest_run
    ]


def fetch_data():
    # call the code in this file... it would time consuming to put it in a function
    # import cda_download
    download_from_weatherdatahub()


def get_latest_mogreps_run():
    hour = datetime.now().hour
    hour = floor(hour/6) * 6
    return f"{hour:02}"


# def convert_and_save_netcdf_to_blob(filepath, parameter_name):
#     cube = iris.load(os.path.join(filepath, f"*{parameter_name}*.grib2"))
#     cube.long_name = parameter_name
#     out_filepath = os.path.join(filepath, f"{parameter_name}.nc")
#     iris.save(cube, out_filepath)
#     return out_filepath

def convert_and_save_netcdf_xr(filepath, parameter_name):
    dss = []
    for grib_filepath in glob(os.path.join(filepath, f"*{parameter_name}*.grib2")):
        print(f"loading grib {grib_filepath}")
        ds = xr.open_dataset(grib_filepath, engine="cfgrib")
        dss.append(ds)
    print(f"got {len(dss)} datasets")
    ds = xr.concat(dss, "number")
    ds = ds.rename_dims({"number": "realization"})
    print(ds)
    print(ds["t2m"])
    out_filepath = os.path.join(filepath, f"{parameter_name}.nc")
    ds.to_netcdf(out_filepath)
    return out_filepath


# COMMAND ----------

main()

# COMMAND ----------


