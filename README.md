# lvr Land DataFeed Crawler

* [內政部:::不動產交易實價查詢服務網](https://plvr.land.moi.gov.tw/DownloadOpenData)

* File Description
  
  |File Name|Description|
  |---------|-----------|
  |land_data_crawler.py|爬取不動產相關 Data Feed|
  |land_data_etl.py|處理不動產 Data Feed|
  |land_data_etl_rdd.py|處理不動產 Data Feed (rdd)|
  |tools/times.py|時間工具|
  |tools/request.py|request 工具|
  |tools/files.py|檔案工具|
  |tools/sp_gadget.py|Spark 相關工具|

* 執行步驟
  
  1. Crawler
    ```bash
      python lvr-land-crawler-spark/land_data_crawler.py {{Crawler mode}} {{Version}} {{file list}}
    ```
    * Note:
      * Crawler mode
        * curr : 當季
        * hist_season : 歷史-季資料
        * hist_date : 歷史-10天資料
      * Version 發布日期
        * ex: 108S2 108年第2季 / 20200521 (YYYYMMDD)
      * File list 選取檔案
        * 以`,`分隔
          * ex: `a_lvr_land_a.csv,f_lvr_land_a.csv`

  2. Spark
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

