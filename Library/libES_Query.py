from __future__ import print_function
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import math
from tqdm import tqdm

from elasticsearch_dsl import A, Search, connections

class es_query:
    dataDict={}
    fieldname=""

    def __init__(self, ES_HOST, ES_INDEX, ES_USERNAME, ES_PASSWORD):
        self.ES_HOST=ES_HOST
        self.ES_INDEX=ES_INDEX
        self.ES_USERNAME=ES_USERNAME
        self.ES_PASSWORD=ES_PASSWORD
        print ("es query object created")

    #def run_search(self, **kwargs):
    def run_search(self,search, source_aggs, inner_aggs={}, size=10000, **kwargs):
        s = search[:0]
        s.aggs.bucket("comp", "composite", sources=source_aggs, size=size, **kwargs)
        for agg_name, agg in inner_aggs.items():
            s.aggs["comp"][agg_name] = agg
        return s.execute()

    def scan_aggs(self,search, source_aggs, inner_aggs={}, size=10000):
        """
        Helper function used to iterate over all possible bucket combinations of
        ``source_aggs``, returning results of ``inner_aggs`` for each. Uses the
        ``composite`` aggregation under the hood to perform this.
        """
        response = self.run_search(search, source_aggs, inner_aggs, size)
        while response.aggregations.comp.buckets:
            for b in response.aggregations.comp.buckets:
                yield b
            if "after_key" in response.aggregations.comp:
                after = response.aggregations.comp.after_key
            else:
                after = response.aggregations.comp.buckets[-1].key
            response = self.run_search(search, source_aggs, inner_aggs, size,after=after)

    def PullUniques(self, field_name):
        self.dataDict.clear()
        self.fieldname=field_name
        # initiate the default connection to elasticsearch
        connections.create_connection(hosts=self.ES_HOST, timeout=360,http_auth=(self.ES_USERNAME,self.ES_PASSWORD))
        print ("    Querying Host, please wait, this could take a few minutes.. ")
        for bucket in self.scan_aggs(Search(index=self.ES_INDEX), {field_name: A("terms", field=field_name)}):
            dictBucket=bucket.to_dict()
            self.dataDict[dictBucket['key'][field_name]]=dictBucket['doc_count']

        print ("    Query Complete! ", len(self.dataDict.keys()), " Records Pulled.")

    # TODO: build folder stucture here instead of using a bash script to do it

    def saveUniques(self, outputFolder, IGNORE_LIST):
        itemCount=0
        if self.fieldname == " ":
            self.fieldname="test"
        strTxtHeader="# Louisiana Cyber Investigator Alliance:LCIA\n# www.la-safe.org\n# Proactive cyber intelligence" + \
                "investigatory work has lead to a growing " + self.fieldname + " list.\n"
        strCSVHeader=self.fieldname+","+"count"+"\n"

        csv_filename = outputFolder + self.fieldname+".csv"
        txt_filename = outputFolder + self.fieldname + ".txt"

        csv_filename=csv_filename.replace(".keyword","")
        txt_filename=txt_filename.replace(".keyword","")

        csv_fileWriter = open(csv_filename, "w")
        txt_fileWriter = open(txt_filename, "w")

        csv_fileWriter.write(strTxtHeader)
        csv_fileWriter.write(strCSVHeader)

        txt_fileWriter.write(strTxtHeader)

        for item in self.dataDict:
            itemCount+=1
            ignoreFlag=False
            for ignoreItem in IGNORE_LIST:
                print ("    ",ignoreItem,":", item)
                if ignoreItem in item:
                    ignoreFlag=True

            print (ignoreItem,":", item)
            if ignoreItem==False:
                strKey=item
                strCount=self.dataDict[item]

                strKey=strKey.replace(',','')
                strKey=strKey.strip()
                if 1 < len(strKey) < 255:
                    csv_line = strKey + "," + str(strCount) + "\n"
                    txt_line = strKey + "\n"
                    csv_fileWriter.write(csv_line)
                    txt_fileWriter.write(txt_line)

        csv_fileWriter.close()
        txt_fileWriter.close()
        print ("     ",self.fieldname, " Data Saved to:", outputFolder, "Items:", itemCount)