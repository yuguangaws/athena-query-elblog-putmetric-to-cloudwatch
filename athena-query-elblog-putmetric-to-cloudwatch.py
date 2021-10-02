#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author:yuguang   498049919@qq.com

import boto3

DATABASE = 'default'
output='s3://aws-athena-query-results-049970088233-us-west-2/'

def queryexec():
    query = "select count(*) from dim;"
    client = boto3.client('athena',region_name='us-west-2')
    queryid = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': output
        }
    )["QueryExecutionId"]
    if queryid is not None:
        pass
    else
        print("The queryid is not generated yet")
        break
    return queryid

def querystatus():
    QID = queryexec()
    client = boto3.client('athena', region_name='us-west-2')
    while true:
     response = client.get_query_execution(
            QueryExecutionId=QID
        )[Status][State]
        if response is not "SUCCEEDED":
            continue
        else:
            break

def queryresult():
    QID = queryexec()
    response = client.get_query_results(QueryExecutionId="%s" %(QID,))
    if response is not None:
        pass
    else
        print("This is query is not finished yet")
        break
    return response["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]

def put_metris():
    count = queryresult()
    countint = int(count)
    client = boto3.client('cloudwatch',region_name='us-west-2')
    client.put_metric_data(Namespace='test-alb-1xx',MetricData=[{'MetricName': '1xx','Value': countint,'Unit': 'Count'}])

if __name__ == '__main__':
    put_metris()
