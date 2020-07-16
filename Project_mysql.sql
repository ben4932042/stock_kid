CREATE DATABASE Project_test;

-- version:1.1.1


-- 每日交易
CREATE TABLE `Project_test`.`daily_trade` (
  `stockiid`                 SMALLINT UNSIGNED   NOT NULL,
  -- 股票代碼
  `date`                     DATE                NOT NULL,
  -- 交易日期
  `open`                     DECIMAL(7,2)        NOT NULL,
   -- 開盤價
  `high`                     DECIMAL(7,2)        NOT NULL,
   -- 最高價
  `low`                      DECIMAL(7,2)        NOT NULL,
   -- 最低價
  `close`                    DECIMAL(7,2)        NOT NULL,
 -- 收盤價  
  `volume`                   BIGINT              NOT NULL);
 -- 交易量

 -- 融資融券 
CREATE TABLE `Project_test`.`margin_trading_short_selling` (
  `stockiid`                SMALLINT UNSIGNED     NOT NULL,
   -- 股票代碼
  `date`                    DATE                  NOT NULL,
   -- 交易日期
  `margin_buy`              MEDIUMINT UNSIGNED    NOT NULL,
   -- 融資買進
  `margin_cell`             MEDIUMINT UNSIGNED    NOT NULL,
   -- 融資賣出
  `margin_remaining`        MEDIUMINT UNSIGNED    NOT NULL,
   -- 融資餘額
   `margin_limit`           MEDIUMINT UNSIGNED    NOT NULL,
   -- 融資限額
  `short_buy`               MEDIUMINT UNSIGNED    NOT NULL,
   -- 融券買進
  `short_cell`              MEDIUMINT UNSIGNED    NOT NULL,
   -- 融券賣出
  `short_remaining`         MEDIUMINT UNSIGNED    NOT NULL,
   -- 融券餘額
  `short_limit`             MEDIUMINT UNSIGNED    NOT NULL,
   -- 融券限額
  `foreign_investment`      INT                       NULL,
   -- 外資買賣超股數
  `investment_bank`         INT                       NULL,
   -- 投信買賣超股數
  `local_company`           INT             NULL);
   -- 自營商買賣超股數


  -- 法人
CREATE TABLE `Project_test`.`legel_person` (
  `stockiid`                SMALLINT UNSIGNED       NOT NULL,
  -- 股票代碼
  `legel_person_number`     SMALLINT UNSIGNED       NOT NULL,
  -- 法人代碼
  `date`                    DATE                    NOT NULL,
  -- 日期
  `legel_person_balance`    INT                         NULL);
  -- 法人買賣超張數
  

   -- 法人索引
CREATE TABLE `Project_test`.`legel_person＿index` (
  `legel_person_index`      VARCHAR(4)              NOT NULL,
  -- 法人代碼
  `legel_person_number`     DATE                    NOT NULL,
  -- 法人代碼索引好
  `legel_person_name`       VARCHAR(8)              NOT NULL,
  -- 法人名
  `legel_person_locate`     VARCHAR(8)              NOT NULL);
  -- 地址(市/區)




  -- 基本面
  CREATE TABLE `Project_test`.`basic` (
  `stockiid`                SMALLINT UNSIGNED         NOT NULL,
  -- 股票代碼
  `season`                  TINYINT UNSIGNED          NOT NULL,
  -- 年＆季
  `net_income`              DECIMAL(7,3) UNSIGNED         NULL,
  -- 稅後淨利
  `total_assets`            DECIMAL(7,2) UNSIGNED         NULL,
  -- 資產總額
  `operating_margin`        DECIMAL(7,3)                  NULL,
  -- 營業毛利
  `account_receivable`      DECIMAL(8,2)                  NULL,
  -- 應收帳款
  `ROA`                     DECIMAL(5,2)                  NULL,
  -- 本益比
  `ROE`                     DECIMAL(5,2)                  NULL,
  -- 股東權益報酬率
  `corrent_assets`          DECIMAL(6,1)                  NULL,
  -- 流動資產
  `current_liabilities`     DECIMAL(8,3)                  NULL,
  -- 流動負債
  `current_ratio`           DECIMAL(7,2)                  NULL,
  -- 速動比
  `total_debt`              DECIMAL(6,2)                  NULL,
  -- 總負債
  `stock_price_per`         DECIMAL(6,2)                  NULL,
  -- 每股淨值
  `share_capital`           DECIMAL(6,2) UNSIGNED         NULL);
  -- 股本




-- 股票資料
  CREATE TABLE `Project_test`.`stock_information` (
  `stockiid`              SMALLINT UNSIGNED           NOT NULL,
  -- 股票代碼
  `stock_name`            VARCHAR(8)                  NOT NULL,
   -- 股票名稱
  `stockiid_locate`       SMALLINT UNSIGNED           NOT NULL,);
   -- 股票地址(市/區)

  
