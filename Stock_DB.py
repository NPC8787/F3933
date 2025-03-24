import sqlite3
import requests
from bs4 import BeautifulSoup
import pandas as pd
import yfinance as yf
import os, time
from datetime import datetime, timedelta
import logging

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('StockDB')

class StockDB:
  def __init__(self, db_path='/content/drive/MyDrive/StockGPT/stock.db', db_start_date='2015-01-01'):
    exist = os.path.exists(db_path) #是否已建立資料庫
    self.db_path = db_path
    self.db_start_date = db_start_date
    self.conn = sqlite3.connect(db_path)
    self.ids = None
    if not exist: #如果未建立資料庫
      logger.info("建立資料庫：" + db_path)
      self.create_tables() # 建立資料表

  # 建立資料表(不存在時才會建立)
  def create_tables(self):
    # Daily 日頻資料表用 "股號+日期" 為主鍵
    # 若要改為自動編號的主鍵,可用：序號 INTEGER PRIMARY KEY AUTOINCREMENT,
    self.conn.execute('''
    CREATE TABLE IF NOT EXISTS 公司 (
        股號 TEXT PRIMARY KEY NOT NULL,
        股名 TEXT,
        產業別 TEXT,
        股本 INTEGER,
        市值 INTEGER
    )
    ''')

    self.conn.execute('''
    CREATE TABLE IF NOT EXISTS 日頻 (
        股號 TEXT,
        日期 TEXT,
        開盤價 REAL,最高價 REAL,最低價 REAL,
        收盤價 READ,還原價 READ,成交量 INTEGER,
        日報酬 REAL,殖利率 REAL,日本益比 REAL,
        股價淨值比 REAL,三大法人買賣超股數 REAL,
        融資買入 REAL,融卷賣出 REAL,
        PRIMARY KEY (股號, 日期)
    )
    ''')   # ↑以股號+日期為主鍵
    self.conn.execute('CREATE INDEX 日期索引 ON 日頻(日期)') #建日期索引


    self.conn.execute('''
    CREATE TABLE IF NOT EXISTS 季頻 (
        股號 TEXT,
        年份 TEXT ,
        季度 REAL,營業收入 REAL,
        營業費用 REAL,稅後淨利 REAL,
        每股盈餘 REAL,
        PRIMARY KEY (股號, 年份, 季度)
    )
    ''')

    self.conn.commit()

  # 更新股票資訊
  def renew(self, if_renew_qu = True):
    self.renew_company() # 公司的基本資訊
    self.renew_daily() # 更新日頻的基本資訊
    if if_renew_qu == True:
      self.renew_quarterly_frequency_basic() # 更新季頻的基本資訊


  # 顯示資料表的結構及索引資訊
  def info(self, table):
    cursor = self.conn.execute(f"PRAGMA table_info({table})")
    column_list = cursor.fetchall()
    print(f"\n【{table}】資料表的結構：")
    for column in column_list:
      print(column)

    cursor = self.conn.execute(f"PRAGMA index_list({table})")
    index_list = cursor.fetchall()
    print("\n索引資訊：")
    for index in index_list:
      print('-------------')
      print(index)
      index_name = index[1]
      cursor = self.conn.execute(f"PRAGMA index_info({index_name})")
      index_columns = cursor.fetchall()
      print('索引欄位：')
      for column in index_columns:
        print(column)

  # 讀取資料 (可有多個資料表, 以逗號分隔, 此時要加上 where 條件。
  #       若有同名欄位, 則要寫成 "資料表.欄位")
  # 參數 psdate (parse_date) 表示是否將日期欄的資料轉為日期型別,預設為 True
  # 傳回 DataFrame
  def get(self, table, select=None, where=None, psdate=False): #參數：資料表, 欄位, 條件式, 解析日期欄
    # 查詢資料
    if not isinstance(table, str): #如果不是字串, 就將元素以逗號組合
      table = ", ".join(table)

    if not select:
      select = "*"
    elif not isinstance(select, str): #如果不是字串, 就將元素以逗號組合
      select = ", ".join(select)


    sql = f"SELECT {select} FROM {table}"
    if where:
      sql += f" WHERE {where}"
    if psdate: # 要解析日期欄位, 將之轉為日期型別
      if table == '日頻':
        df = pd.read_sql(sql, self.conn, parse_dates=['日期'])
      elif table == '季頻':
        sql = '''
        SELECT 股號, 
            營業收入, 
            營業費用, 
            稅後淨利, 
            每股盈餘,
            strftime('%Y-%m-%d', 年份 || '-' || 
            CASE 
                WHEN 季度 = 'Q1' THEN '03' 
                WHEN 季度 = 'Q2' THEN '06'
                WHEN 季度 = 'Q3' THEN '09'
                WHEN 季度 = 'Q4' THEN '12'
            END || '-01') as 日期
            
        FROM 季頻
        ORDER BY 股號 ASC, 日期 DESC'''
        df = pd.read_sql(sql, self.conn, parse_dates=['日期']) 
        column_order = ['股號', '日期', '營業收入', '營業費用', '稅後淨利', '每股盈餘']
        df = df[column_order]
        
    else:
      df = pd.read_sql(sql, self.conn)
    return df

  # 關閉資料庫
  def close(self):
    self.conn.close()


  ##############################
  ## 更新股票資訊的相關方法 ##
  ##############################

  #讀取最近一天所有股票的日頻資料, 以取得最新的股號及股名清單
  #欄位："股號","股名","成交量","成交金額","開盤價","最高價","最低價","收盤價","漲跌價差","成交筆數"

  # 上市股票
  def stock_name(self):
    # print(self.ids)
    if self.ids is not None:
      return self.ids
    logger.info("線上讀取股號、股名、及產業別")
    data=[]
    try:
      response=requests.get('https://isin.twse.com.tw/isin/C_public.jsp?strMode=2')
      response.raise_for_status()  # 確保請求成功
      url_data=BeautifulSoup(response.text, 'html.parser')
      stock_company=url_data.find_all('tr')
      for i in stock_company[2:]:
          j=i.find_all('td')
          if len(j) < 5:  # 確保有足夠的列
            continue
          l=j[0].text.split('\u3000')
          if len(l) == 2 and len(l[0].strip()) == 4 and l[0].strip().isdigit():
              stock_id,stock_name = l
              industry = j[4].text.strip()
              data.append([stock_id.strip(),stock_name,industry])
          else:
              # 不是股票代碼或格式不符的情況，可能是其他類型的證券
              continue
      df = pd.DataFrame(data, columns=['股號','股名','產業別'])
      self.ids = df
      return df
    except Exception as e:
      logger.error(f"讀取股票資料失敗: {e}")
      # 如果出錯，返回空的 DataFrame
      return pd.DataFrame(columns=['股號','股名','產業別'])

  #更新公司基本資料, 預設只會加入新上市的公司, 若將參數all設為Ture則全部更新
  def renew_company(self, all=False):
    try:
      df_old = self.get("公司", '股號,股名,產業別')
      if all or df_old.empty: # 先刪除全部, 再重新讀取
        self.conn.execute("DELETE FROM 公司")
        df = self.stock_name()
        logger.info(f'更新所有的公司: {len(df)} 筆')
      else:
        df_new = self.stock_name()
        mask = df_new['股號'].isin(df_old['股號']) # 建立在new存在,在old也存在的遮罩
        df = df_new[~mask] #反轉遮罩, 取出在new有在old沒有的資料
        logger.info(f'要更新的公司: {len(df)} 筆')

      if len(df) == 0:
        logger.info('沒有新公司需要更新')
        return
        
      # 使用事務處理
      with self.conn:
        for id,name,industry in zip(df['股號'],df['股名'],df['產業別']):
          try:
            # 添加重試機制
            max_retries = 3
            for attempt in range(max_retries):
              try:
                stock = yf.Ticker(id+".TW")
                break
              except Exception as e:
                if attempt < max_retries - 1:
                  logger.warning(f"嘗試獲取 {id} 資料時出錯，重試中... ({attempt+1}/{max_retries})")
                  time.sleep(2)  # 等待一段時間後重試
                else:
                  raise e
                
            # 安全地獲取股票資訊
            stock_sharesOutstanding = stock.info.get('sharesOutstanding', None)
            stock_marketCap = stock.info.get('marketCap', None)
    
            self.conn.execute("INSERT INTO 公司 values(?,?,?,?,?)",
                    (id,name,industry,stock_sharesOutstanding,stock_marketCap))
            logger.info(f"已更新公司: {id} - {name}")
          except Exception as e:
            logger.error(f"更新公司 {id} 時出錯: {e}")
    except Exception as e:
      logger.error(f"renew_company 方法出錯: {e}")

  def quarter_to_int(self, year, quarter):
    quarter_dict = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
    return int(year) * 10 + quarter_dict[quarter]

  # 更新季頻的基本資訊
  def renew_quarterly_frequency_basic(self):
    try:
      #找出最後更新日期
      cursor = self.conn.execute('SELECT 年份, 季度 FROM 季頻 ORDER BY 年份 DESC, 季度 DESC LIMIT 1')
      m_date = cursor.fetchone()
      if m_date:
        latest_year, latest_quarter = m_date
        logger.info(f'季頻基本資料的最後更新日：{latest_year} {latest_quarter}')
      else:
        logger.info('季頻表中沒有資料')
        latest_year, latest_quarter = None, None
        
      today = datetime.now()
      
      # 根據當前日期確定要獲取的財報類型
      # Q1（一季報）：5/15 前後公告
      # Q2（二季報）：8/14 前後公告
      # Q3（三季報）：11/14 前後公告
      # Q4（年報）：隔年 3/31 前公告
      
      q1_release = datetime(today.year, 5, 15)
      q2_release = datetime(today.year, 8, 14)
      q3_release = datetime(today.year, 11, 14)
      q4_release = datetime(today.year, 3, 31)
      
      # 修正季度判斷邏輯
      if datetime(today.year, 3, 31) <= today < datetime(today.year, 5, 15):
          # 3/31 - 5/15: 去年第四季數據已公布
          report_type = "Q4"
          report_year = str(today.year - 1)
      elif datetime(today.year, 5, 15) <= today < datetime(today.year, 8, 14):
          # 5/15 - 8/14: 今年第一季數據已公布
          report_type = "Q1"
          report_year = str(today.year)
      elif datetime(today.year, 8, 14) <= today < datetime(today.year, 11, 14):
          # 8/14 - 11/14: 今年第二季數據已公布
          report_type = "Q2"
          report_year = str(today.year)
      elif today >= datetime(today.year, 11, 14):
          # 11/14 後: 今年第三季數據已公布
          report_type = "Q3"
          report_year = str(today.year)
          
      logger.info(f"當前可獲取的最新財報: {report_year} {report_type}")
      
      # 判斷是否需要更新
      if latest_year is not None and latest_quarter is not None:
        latest_value = self.quarter_to_int(latest_year, latest_quarter)
        current_value = self.quarter_to_int(report_year, report_type)
        
        if current_value <= latest_value:
          logger.info("不需要更新季頻資料")
          return
      
      #更新季頻資料表
      logger.info('開始更新季頻資料')
      
      df = self.stock_name()
      success_count = 0
      total_count = len(df)
      
      with self.conn:  # 使用事務
        for id, name in zip(df['股號'],df['股名']):
          try:
            df_data=[]
            url = [f'https://tw.stock.yahoo.com/quote/{id}.TW/income-statement',
                    f'https://tw.stock.yahoo.com/quote/{id}.TW/eps']
            
            # 添加重試機制
            max_retries = 3
            for attempt in range(max_retries):
              try:
                df = self.url_find(url[0])
                if df.empty:
                  raise ValueError(f"無法獲取 {id} 的損益表資料")
                break
              except Exception as e:
                if attempt < max_retries - 1:
                  logger.warning(f"嘗試獲取 {id} 損益表時出錯，重試中... ({attempt+1}/{max_retries})")
                  time.sleep(2)  # 等待一段時間後重試
                else:
                  raise ValueError(f"無法獲取 {id} 的損益表資料: {e}")
            
            # 處理得到的 DataFrame
            logger.info(f"處理 {id} 的數據")
            
            # 檢查 DataFrame 是否為空
            if df.empty:
              logger.warning(f"股票 {id} 的損益表資料為空，跳過")
              continue
              
            df = df.transpose()
            # 檢查資料結構
            if len(df) < 1:
              logger.warning(f"股票 {id} 的損益表資料結構異常，跳過")
              continue
              
            df.columns = df.iloc[0]
            df = df[1:]
            df.insert(0,'年度/季別',df.index)
            df.columns.name = None
            df.reset_index(drop=True, inplace=True)
            df_data.append(df)
    
            # 季EPS表
            for attempt in range(max_retries):
              try:
                df = self.url_find(url[1])
                if df.empty:
                  raise ValueError(f"無法獲取 {id} 的EPS資料")
                break
              except Exception as e:
                if attempt < max_retries - 1:
                  logger.warning(f"嘗試獲取 {id} EPS時出錯，重試中... ({attempt+1}/{max_retries})")
                  time.sleep(2)  # 等待一段時間後重試
                else:
                  raise ValueError(f"無法獲取 {id} 的EPS資料: {e}")
                  
            df_data.append(df)
    
            # 檢查兩個 DataFrame 是否都有數據
            if any(d.empty for d in df_data):
              logger.warning(f"股票 {id} 的部分數據為空，跳過")
              continue
    
            # 將兩個 DataFrame 按列名合併
            try:
              combined_df = df_data[0].merge(df_data[1], on='年度/季別')
              
              # 檢查合併後的 DataFrame 是否包含所需的所有列
              required_columns = ['年度/季別', '營業收入', '營業費用', '稅後淨利', '每股盈餘']
              if not all(col in combined_df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in combined_df.columns]
                logger.warning(f"股票 {id} 的合併數據缺少列: {missing_cols}，跳過")
                continue
                
              # 安全地選擇列
              combined_df = combined_df[required_columns]
              
              # 分割年份和季度
              combined_df[['年份', '季度']] = combined_df['年度/季別'].str.split(' ', expand=True)
              combined_df.drop(columns=['年度/季別'], inplace=True)
              
              # 確保所有數據列均為數值類型
              for col in ['營業收入', '營業費用', '稅後淨利', '每股盈餘']:
                combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
    
              # 重新排列列的順序
              combined_df = combined_df[['年份', '季度', '營業收入', '營業費用', '稅後淨利', '每股盈餘']]
              combined_df.insert(0, '股號', id)   # 加入股號欄
              
              # 使用 INSERT OR REPLACE 語法來更新資料
              combined_df.to_sql('季頻', self.conn, if_exists='append', index=False)
              success_count += 1
              logger.info(f"成功更新 {id} 的季頻資料")
            except Exception as e:
              logger.error(f"處理 {id} 的季頻資料時出錯: {e}")
              continue
          except Exception as e:
            logger.error(f"更新 {id} 的季頻資料時出錯: {e}")
            continue
      
      logger.info(f"季頻資料更新完成, 成功處理 {success_count}/{total_count} 個股票")
      return
    except Exception as e:
      logger.error(f"renew_quarterly_frequency_basic 方法出錯: {e}")
   

  def url_find(self, url):
    try:
      words = url.split('/')
      k = words[-1]
      
      # 使用requests取得網頁內容
      response = requests.get(url, timeout=10)
      response.raise_for_status()  # 確保請求成功
      html = response.content

      # 使用Beautiful Soup解析HTML內容
      soup = BeautifulSoup(html, 'html.parser')

      # 找到表格的表頭
      table_soup = soup.find('section', {'id': f'qsp-{k}-table'})
      if not table_soup:
        logger.warning(f"在 {url} 中找不到指定的表格區域")
        return pd.DataFrame()  # 返回空的 DataFrame
        
      table_fields = table_soup.find('div', class_='table-header')
      if not table_fields:
        logger.warning(f"在 {url} 中找不到表頭")
        return pd.DataFrame()  # 返回空的 DataFrame

      table_fields_lines = list(table_fields.stripped_strings)
      if len(table_fields_lines) < 2:
        logger.warning(f"在 {url} 中表頭不完整")
        return pd.DataFrame()  # 返回空的 DataFrame
        
      data_rows = table_soup.find_all('li', class_='List(n)')

      # 解析資料行內容
      data = []
      for row in data_rows:
          row_data = list(row.stripped_strings)
          if len(row_data) < 2:
            continue  # 跳過資料不完整的行
          # 移除數字中的逗號
          row_data[1] = row_data[1].replace(',', '')
          data.append(row_data[0:2])

      # 建立 DataFrame
      if len(data) == 0:
        logger.warning(f"在 {url} 中沒有找到數據行")
        return pd.DataFrame()  # 返回空的 DataFrame
        
      df = pd.DataFrame(data, columns=table_fields_lines[0:2])
      return df
    except Exception as e:
      logger.error(f"url_find 方法出錯 (URL: {url}): {e}")
      return pd.DataFrame()  # 返回空的 DataFrame

  # 日頻股價資料
  def stock_price(self, stock_list, start_date):
    try:
      # 下載資料
      df = yf.download(stock_list, start=start_date, auto_adjust=False, multi_level_index=False)

      if len(df) > 0: # 如果有下載到資料
          # 轉換資料
          data_list = []
          for stock in stock_list:
              try:
                stock_df = df.xs(stock, axis=1, level=1).copy()
                stock_df['Stock_Id'] = stock.replace('.TW', '')
                data_list.append(stock_df)
              except Exception as e:
                logger.error(f"處理股票 {stock} 的數據時出錯: {e}")

          if not data_list:
            logger.warning("沒有成功處理任何股票數據")
            return pd.DataFrame()

          yf_df = pd.concat(data_list).reset_index()

          # 重新排列欄位
          expected_columns = ['Date', 'Stock_Id', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
          if not all(col in yf_df.columns for col in expected_columns):
            missing_cols = [col for col in expected_columns if col not in yf_df.columns]
            logger.error(f"股價數據缺少必要的列: {missing_cols}")
            return pd.DataFrame()
            
          yf_df = yf_df[expected_columns]
          yf_df.rename(columns={  # 修改欄位名稱以便對應到資料表
              'Stock_Id':'股號','Date':'日期','Open':'開盤價','High':'最高價','Low':'最低價',
              'Close':'收盤價', 'Adj Close':'還原價','Volume':'成交量',}, inplace=True)
          # ↓將TimeStamp資料改為如 "2022-02-03" 的字串
          yf_df['日期'] = yf_df['日期'].dt.strftime('%Y-%m-%d')

          return yf_df
      else:
        logger.warning(f"從 {start_date} 開始沒有下載到任何股價數據")
        return pd.DataFrame()
    except Exception as e:
      logger.error(f"stock_price 方法出錯: {e}")
      return pd.DataFrame()

  # 進階日頻資料下載
  def stock_advanced(self, date):
    try:
      urls = [
          f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?date={date}&selectType=ALL&response=json",
          f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date}&selectType=ALLBUT0999&response=json",
          f"https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={date}&selectType=STOCK&response=json"
      ]
      dfs = []
      
      # 取得本益比資料
      try:
        response = requests.get(urls[0], timeout=10)
        response.raise_for_status()
        json_data1 = response.json()
        # 有資料才執行程式
        if 'stat' in json_data1 and json_data1['stat'] == 'OK' and 'data' in json_data1 and json_data1['data']:
            df1 = pd.DataFrame(json_data1['data'], columns=json_data1['fields'])
            df1 = df1[['證券代號','殖利率(%)','本益比','股價淨值比']]
            df1.insert(1, '日期', datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d'))
            df1.rename(columns={
                    '證券代號':'股號','殖利率(%)':'殖利率','本益比':'日本益比'
                    }, inplace=True)
            dfs.append(df1)
      except Exception as e:
        logger.error(f"獲取本益比資料時出錯 (日期: {date}): {e}")
        
      time.sleep(2)
      
      # 取得法人買賣超資料
      try:
        response = requests.get(urls[1], timeout=10)
        response.raise_for_status()
        json_data2 = response.json()
        if 'stat' in json_data2 and json_data2['stat'] == 'OK' and 'data' in json_data2 and json_data2['data']:
            df2 = pd.DataFrame(json_data2['data'], columns=json_data2['fields'])
            df2 = df2[['證券代號','三大法人買賣超股數']]
            df2.rename(columns={
                    '證券代號':'股號'
                    }, inplace=True)
            dfs.append(df2)
      except Exception as e:
        logger.error(f"獲取法人買賣超資料時出錯 (日期: {date}): {e}")
        
      time.sleep(2)
      
      # 取得融資融券資料
      try:
        response = requests.get(urls[2], timeout=10)
        response.raise_for_status()
        json_data3 = response.json()
        if 'stat' in json_data3 and json_data3['stat'] == 'OK' and 'tables' in json_data3 and len(json_data3['tables']) > 1 and 'data' in json_data3['tables'][1] and json_data3['tables'][1]['data']:
            data = pd.DataFrame(json_data3['tables'][1]['data'])
            if len(data.columns) >= 10:  # 確保有足夠的列
              df3 = data.iloc[:, [0, 2, 9]]
              df3.columns = ['股號', '融資買入', '融卷賣出']
              dfs.append(df3)
      except Exception as e:
        logger.error(f"獲取融資融券資料時出錯 (日期: {date}): {e}")
        
      time.sleep(2)

      # 檢查是否有數據
      if not dfs:
        logger.warning(f"沒有獲取到任何進階日頻數據 (日期: {date})")
        return pd.DataFrame()
        
      try:
        # 使用 reduce 函數從左到右連續合併 DataFrames
        from functools import reduce
        merged_df = reduce(lambda left, right: pd.merge(left, right, on='股號', how='inner'), dfs)
        return merged_df
      except Exception as e:
        logger.error(f"合併進階日頻數據時出錯 (日期: {date}): {e}")
        return pd.DataFrame()
    except Exception as e:
      logger.error(f"stock_advanced 方法出錯 (日期: {date}): {e}")
      return pd.DataFrame()

  # 更新日頻的基本資訊
  def renew_daily(self):
    try:
      #找出最後更新日期
      cursor = self.conn.execute('SELECT MAX(日期) FROM 日頻 WHERE 開盤價 IS NOT NULL')
      m_date = cursor.fetchone()[0]
      logger.info(f'日頻基本資料的最後更新日：{m_date}')
      
      if not m_date:
        start_
