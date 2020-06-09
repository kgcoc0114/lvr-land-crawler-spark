# lvr Land DataFeed Crawler

* [內政部:::不動產交易實價查詢服務網](https://plvr.land.moi.gov.tw/DownloadOpenData)

* File Description
  
  |File Name|Description|
  |---------|-----------|
  |land_data_crawler.py|爬取不動產相關datafeed|
  |land_data_etl.py|處理不動產datafeed|
  |land_data_etl_rdd.py|處理不動產datafeed(rdd)|
  |tools/times.py|時間工具|
  |tools/request.py|request工具|
  |tools/files.py|檔案工具|
  |tools/sp_gadget.py|spark相關工具|

* 執行步驟
  
  1. 
    ```bash
      python lvr-land-crawler-spark/land_data_crawler.py
    ```
  2. 
    ```bash
      spark-submit --py-files lvr-land-crawler-spark/dependency.zip lvr-land-crawler-spark/land_data_etl.py
    ```

* 若於`local`執行且`local`未有Spark環境，請加上
  ```python
  import findspark
  findspark.init()
  ```
* dependency.zip
  * 因有import 自定義 module
  * 需匯出zip
    ```bash
    cd lvr-land-crawler-spark
    sudo zip dependency.zip tools/*
    ```

