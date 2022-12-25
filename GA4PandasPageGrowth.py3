#!/usr/bin/env python3
"""
 Calculates growth and trends for unique page counts from Analytics Data API v1 (newer GA4 model)
 Uses gapandas4 library so we can use dataframes

 Starting point attribution:
 https://developers.google.com/analytics/devguides/reporting/data/v1/quickstart-client-libraries
"""

#
# from inside venv:
# pip3 install google-analytics-data
# pip3 install --upgrade oauth2client
# pip3 install gapandas4
# pip3 install tabulate
# pip3 freeze | tee requirements.txt
#
import sys
import traceback
import argparse

import gapandas4 as gp

def get_unique_pagecount_report(jsonKeyFilePath,property_id,startDateStr,endDateStr):
    """Runs a report on a Google Analytics GA4 property."""

    request = gp.RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[gp.Dimension(name="pagePath")],
        metrics=[gp.Metric(name="activeUsers")],
        date_ranges=[gp.DateRange(start_date=startDateStr, end_date=endDateStr)],
        order_bys=[ gp.OrderBy(metric = {'metric_name': 'activeUsers'}, desc = False) ]
    )
    df = gp.query(jsonKeyFilePath,request,report_type="report")
    #print(df.head)

    # filter out all rows that contain special chars, are wordpress special paths, or len<16
    targets = ['?','&',"/category/","/page/","/tag/"]
    df = df[df.apply(lambda r: any([not kw in r[0] for kw in targets]), axis=1)]
    df = df[df.apply(lambda r: any([len(r[0])>16 for kw in targets]), axis=1)]

    # make sure count is integer for sorting later
    df['activeUsers'] = df['activeUsers'].astype(str).astype(int)

    # sort by count
    df = df.sort_values('activeUsers',ascending=False)

    return df

def synthesize_older_columns(response_latest,response_older):
  # do left join on pagePath, which gives us new count and old count in same dataframe
  response_latest = response_latest.merge(response_older,how='left',on='pagePath',suffixes=('','_old') )

  # synthesize delta (new-old) to see absolute winners/losers
  response_latest['delta'] = response_latest.apply(lambda row: row.activeUsers - row.activeUsers_old, axis=1)

  # sythesize delta percent (delta/new count) to see trends of growth
  #response_latest['delta'] = response_latest['delta'].ffill()
  response_latest['deltaPercent'] = response_latest.apply(lambda row: (row.delta/row.activeUsers)*100, axis=1)

  #print(response_latest.head())
  return response_latest


def main():

  examples = '''USAGE:
 jsonKeyFile googleGA4PropertyID [reportingDays=30]

 jsonKeyFile is the Google service account json key file
 googleGA4PropertyID can be seen by going to Google Analytics, Admin>Property Settings
 reportingDays is the number of days to rollup into reporting block (today-reportingDays)


 EXAMPLES:
 my.json 123456789
 my.json 123456789 14
'''

  # define arguments
  ap = argparse.ArgumentParser(description="Calculate growth/trends from Analytics",epilog=examples,formatter_class=argparse.RawDescriptionHelpFormatter)
  ap.add_argument('key', help="json key of Google service account")
  ap.add_argument('propertyId', help="GA4 propertyID from Google Analytics (Admin>Property Settings)")
  ap.add_argument('-d', '--days', default="30",help="number of days in reporting window")
  args = ap.parse_args()

  print(f"service account json={args.key}, Google Analytics propertyID={args.propertyId}, reporting window={args.days} days")
  #client = initialize_ga4_analyticsreporting(args.key)

  #sample_run_report(client,args.propertyId)
  
  # get unique page counts per reporting day width
  ndays=int(args.days)
  response_latest = get_unique_pagecount_report(args.key, args.propertyId, startDateStr=f"{ndays}daysAgo", endDateStr="0daysAgo")
  response_older  = get_unique_pagecount_report(args.key, args.propertyId, startDateStr=f"{ndays*2}daysAgo", endDateStr=f"{ndays+1}daysAgo")
  print(f"lastest reporting window: 0daysAgo -> {ndays}daysAgo")
  print(f"older   reporting window: {ndays+1}daysAgo -> {ndays*2}daysAgo")
  print()

  # synthesize columns with older datapoints to construct delta and deltaPercent
  response_latest = synthesize_older_columns(response_latest,response_older)


  # how many losers/winners to display
  nrows=25

  # sort by biggest absolute winners
  response_latest = response_latest.sort_values('activeUsers',ascending=False)

  # show losers and winners in terms of absolute hits
  print("====BIGGEST LOSERS======")
  print("delta,count,path")
  print(response_latest[['activeUsers','pagePath']].tail(nrows).to_string(index=False))

  print("====BIGGEST WINNERS======")
  print("delta,count,path")
  print(response_latest[['activeUsers','pagePath']].head(nrows).to_string(index=False))

  # sort by biggest percent winners to show trends
  response_latest = response_latest.sort_values('deltaPercent',ascending=False)
  # remove older entries that contain 'NaN' because that means we do not have enough data
  response_latest = response_latest.dropna(subset=['activeUsers_old','deltaPercent'])
  # make percentage human readable
  response_latest['prettyPercent'] = response_latest['deltaPercent'].astype(int).astype(str).add("%")

  # show losers and winners in terms of percent growth (% of total)
  print("====TRENDING DOWN======")
  print("growth%,newcount,oldcount,path")
  print(response_latest[['prettyPercent','activeUsers','activeUsers_old','pagePath']].tail(nrows).to_string(index=False))

  print("====TRENDING UP======")
  print("growth%,newcount,oldcount,path")
  print(response_latest[['prettyPercent','activeUsers','activeUsers_old','pagePath']].head(nrows).to_string(index=False))

if __name__ == '__main__':
  main()