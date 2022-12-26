# Page growth/trend report from Google Analytics Data API v1 and GA4 Property using gapandas4

[Google Analytics](https://analytics.google.com/) makes it easy to view your page counts for an arbitrary date range, but what is more difficult is comparing these results to an older window of data to see:

* Which pages have grown/lost in absolute counts
* Which pages are trending growth/loss in terms of their percent (up-and-comers)

This [GA4PandasPageGrowth.py3](GA4PandasPageGrowth.py3) Python script uses the [gapandas4 module](https://github.com/practical-data-science/gapandas4) to:

* Query a window of GA4 data (default=today-30daysAgo), create [pandas DataFrame](https://www.geeksforgeeks.org/python-pandas-dataframe/)
* Query the preceding window of GA4 data (default=30daysAgo-60daysAgo), create pandas DataFrame
* Merge older DataFrame page counts, sythesize delta and deltaPercentage
* Sort by absolute page counts, show biggest absolute winners/losers
* Sorty by delta percent changes, show biggest trending winners/losers

This can help you fine-tune your content creation, and invest in content that is showing the largest potential.

# Google Analytics GA4 is future model

The [gapandas4 module](https://github.com/practical-data-science/gapandas4) uses the latest [Google Analytics Data API v1](https://developers.google.com/analytics/devguides/reporting/data/v1) (GA4 using propertyId) data source, and not the deprecated [Google Analytics Reporting API v4](https://developers.google.com/analytics/devguides/reporting/core/v4) (UA using viewId), which is scheduled to be turned off in June 2023.


## Prerequisites

* Google GCP
  * [Create GCP Project](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py) using this quickstart
  * [Enable Analytics Data API for project](https://console.cloud.google.com/start/api?id=analyticsreporting.googleapis.com&credential=client_key) enable Analytics Data API for this project
  * Create Google Service Account (GSA), download json key for auth later
* Google Analytics
  * Link Google Analytics to your web site, decorate site pages with tracker
  * Admin > "Property" > Propery Settings, copy down your propery ID for reporting later
  * Admin > Account Access Management, add GSA in 'Viewer' role so it can query data


## Run report against Google Analytics GA4

### Prepare environment

```
# make sure python3 venv is installed
sudo apt-get update
sudo apt-get install software-properties-common python3 python3-dev python3-pip python3-venv curl git -y

git clone https://github.com/fabianlee/ga4-pandas-py3-page-growth-trend.git
cd ga4-pandas-py3-page-growth-trend

# create virtual env for isolated libs
python3 -m venv .
source bin/activate

# install module dependencies into virtual env
pip3 install -r requirements.txt
```

### Invoke Script

```
# place json key into this directory

# invoke report generator
./GA4PandasPageGrowth.py3 <jsonKeyFile> <analyticsPropertyID>

# width of report window can be changed (default=30 days)
./GA4PandasPageGrowth.py3 <jsonKeyFile> <analyticsPropertyID> -d 14
```



## REFERENCES

* https://developers.google.com/analytics/devguides/reporting/data/v1/quickstart-client-libraries#python
* https://googleapis.dev/python/google-auth/latest/user-guide.html
* https://googleapis.dev/python/google-api-core/latest/auth.html
* https://www.oncrawl.com/technical-seo/forecast-search-traffic-python-ga4/
* https://practicaldatascience.co.uk/data-science/how-to-query-the-google-analytics-data-api-for-ga4-with-python
* https://github.com/tanyazyabkina/GA4_API_python/blob/main/GA4%20Python%20Report.ipynb
