import os

class csvReport:

    def __init__(self, ReportFolder):
        print ("Report Builder Created..")
        self.reportFolder=ReportFolder
        self.processFiles()

    def processFiles(self):
        path = self.reportFolder

        files = os.listdir(path)

        # for f in files:
        #     fileWithPath=path + "/" + f
        #     if ".csv" in fileWithPath:
        #         print(fileWithPath)
        #         fileData=self.readFile(fileWithPath)
        #         self.sortData(fileData, fileWithPath)

    def readFile(self,fileWithPath):
        dataDict = {}

        fileReader=open(fileWithPath, "r")
        fileData=fileReader.readlines()
        for line in fileData:
            if line.startswith("# "):
                continue
            else:
                try:
                    if "count" in line:
                        continue
                    else:
                        line=line.strip()
                        splitLine=line.split(",")
                        dataDict[splitLine[0]]=int(splitLine[1])
                except:
                    print ("ERROR:", line)
        return dataDict.copy()

    def sortData(self, fileData, filename):
        count=1
        top25=[]
        fileToWrite=filename.replace(".csv","")
        fileToWrite=fileToWrite+"-sorted.csv"
        print ("Filename:", fileToWrite)
        sortWriter=open(fileToWrite,"w")

        sorted_x = sorted(fileData.items(), key=lambda kv: kv[1], reverse=True)
        for item in sorted_x:
            lineToWrite=str(item)+"\n"
            if count < 25:
                sortWriter.write(lineToWrite)
                count+=1
                print ("Writing:",lineToWrite)
        sortWriter.close()





