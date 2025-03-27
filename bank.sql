CREATE TABLE bank_accounts ( --账户表
    account_id   NUMBER PRIMARY KEY,     -- 账户ID（主键）
    account_name VARCHAR2(50) NOT NULL,  -- 账户名称
    balance      NUMBER(15,2) NOT NULL CHECK (balance >= 0), -- 余额（不可透支）
    created_at   TIMESTAMP DEFAULT SYSTIMESTAMP -- 创建时间
);
-- 1001	Alice	5000.00	2023-10-01 09:00:00
CREATE SEQUENCE seq_account_id START WITH 1001; -- 账户ID自增序列
CREATE INDEX idx_accounts_name ON bank_accounts(account_name);
INSERT INTO bank_accounts (account_id, account_name, balance)
VALUES (seq_account_id.NEXTVAL, 'Alice', 5000.00);
INSERT INTO bank_accounts (account_id, account_name, balance)
VALUES (seq_account_id.NEXTVAL, 'Bob', 3000.00);
INSERT INTO bank_accounts (account_id, account_name, balance)
VALUES (seq_account_id.NEXTVAL, 'Charlie', 2000.00);
commit;

CREATE TABLE transactions ( --交易记录表
    transaction_id   NUMBER PRIMARY KEY,      -- 交易ID（主键）
    account_id       NUMBER NOT NULL,         -- 关联账户ID
    target_account_id NUMBER,                 -- 目标账户ID（转账时使用）
    transaction_type VARCHAR2(10) NOT NULL,   -- 交易类型（DEPOSIT/WITHDRAW/TRANSFER）
    amount           NUMBER(15,2) NOT NULL CHECK (amount > 0), -- 金额
    transaction_time TIMESTAMP DEFAULT SYSTIMESTAMP, -- 交易时间
    FOREIGN KEY (account_id) REFERENCES bank_accounts(account_id)
);
-- 5001  1001  NULL  DEPOSIT   2000.00 2023-10-01 10:00:00
-- 5002  1002  NULL  WITHDRAW  500.00  2023-10-01 10:05:00
-- 5003  1001  1002  TRANSFER  1000.00 2023-10-01 10:10:00
CREATE SEQUENCE seq_transaction_id START WITH 5001; -- 交易ID自增序列
CREATE INDEX idx_transactions_account ON transactions(account_id, transaction_time);
INSERT INTO transactions (transaction_id, account_id, transaction_type, amount)
VALUES (seq_transaction_id.NEXTVAL, 1001, 'DEPOSIT', 2000.00); -- Alice 存款 2000
INSERT INTO transactions (transaction_id, account_id, transaction_type, amount)
VALUES (seq_transaction_id.NEXTVAL, 1002, 'WITHDRAW', 500.00); -- Bob 取款 500
INSERT INTO transactions (transaction_id, account_id, target_account_id, transaction_type, amount)
VALUES (seq_transaction_id.NEXTVAL, 1001, 1002, 'TRANSFER', 1000.00); -- Alice 转账 1000 给 Bob
commit;

CREATE TABLE error_logs ( --增加日志表
    log_id        NUMBER PRIMARY KEY,
    error_time    TIMESTAMP DEFAULT SYSTIMESTAMP,
    error_message VARCHAR2(4000),
    procedure_name VARCHAR2(100)
);
CREATE SEQUENCE seq_log_id;
commit;

CREATE OR REPLACE PROCEDURE create_account ( --创建账户 核心PL/SQL代码
    p_account_name IN VARCHAR2,
    p_initial_balance IN NUMBER
) AS
BEGIN
    INSERT INTO bank_accounts (account_id, account_name, balance) --插入账户表
    VALUES (seq_account_id.NEXTVAL, p_account_name, p_initial_balance);
    COMMIT;
END;

CREATE OR REPLACE PROCEDURE deposit ( --存款
    p_account_id IN NUMBER,
    p_amount     IN NUMBER
) AS
BEGIN
    UPDATE bank_accounts  --插入账户表
    SET balance = balance + p_amount 
    WHERE account_id = p_account_id;
    
    INSERT INTO transactions (transaction_id, account_id, transaction_type, amount) --插入交易记录表
    VALUES (seq_transaction_id.NEXTVAL, p_account_id, 'DEPOSIT', p_amount);
    COMMIT;
END;


CREATE OR REPLACE PROCEDURE withdraw ( --取款（禁止透支） 强化事务与异常记录
    p_account_id IN NUMBER,
    p_amount     IN NUMBER
) AS
    v_balance NUMBER;
    PRAGMA AUTONOMOUS_TRANSACTION; -- 防止主事务锁扩散
BEGIN
    SELECT balance INTO v_balance 
    FROM bank_accounts 
    WHERE account_id = p_account_id FOR UPDATE;
    
    IF v_balance >= p_amount THEN 
        UPDATE bank_accounts SET balance = balance - p_amount 
        WHERE account_id = p_account_id;
        
        INSERT INTO transactions (transaction_id, account_id, transaction_type, amount)
        VALUES (seq_transaction_id.NEXTVAL, p_account_id, 'WITHDRAW', p_amount);
        COMMIT;
    ELSE
        INSERT INTO error_logs (log_id, error_message, procedure_name)
        VALUES (seq_log_id.NEXTVAL, 'Insufficient balance for account ' || p_account_id, 'WITHDRAW');
        COMMIT;
        RAISE_APPLICATION_ERROR(-20001, 'Insufficient balance');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO error_logs (log_id, error_message, procedure_name)
        VALUES (seq_log_id.NEXTVAL, SQLERRM, 'WITHDRAW');
        ROLLBACK;
        RAISE;
END;


CREATE OR REPLACE PROCEDURE transfer ( --转账
    p_from_account_id IN NUMBER,
    p_to_account_id   IN NUMBER,
    p_amount          IN NUMBER
) AS
BEGIN
    SAVEPOINT start_transfer; -- 事务保存点
    
    -- 转出
    UPDATE bank_accounts SET balance = balance - p_amount 
    WHERE account_id = p_from_account_id 
    AND balance >= p_amount;
    
    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Transfer failed: Insufficient funds');
    END IF;
    
    -- 转入
    UPDATE bank_accounts SET balance = balance + p_amount 
    WHERE account_id = p_to_account_id;
    
    -- 记录交易
    INSERT INTO transactions (transaction_id, account_id, target_account_id, transaction_type, amount)
    VALUES (seq_transaction_id.NEXTVAL, p_from_account_id, p_to_account_id, 'TRANSFER', p_amount);
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK TO start_transfer;
        INSERT INTO error_logs (log_id, error_message, procedure_name)
        VALUES (seq_log_id.NEXTVAL, SQLERRM, 'TRANSFER');
        RAISE;
END;

--导出到CSV（使用UTL_FILE）
CREATE OR REPLACE DIRECTORY BANK_DATA_DIR AS '/opt/oracle/data';
GRANT READ, WRITE ON DIRECTORY BANK_DATA_DIR TO your_user;
--导出到CSV（使用UTL_FILE）
CREATE OR REPLACE PROCEDURE export_to_csv (
    p_table_name  IN VARCHAR2,
    p_file_name   IN VARCHAR2
) AS
    v_file  UTL_FILE.FILE_TYPE;
    v_query VARCHAR2(4000);
    v_data  VARCHAR2(4000);
BEGIN
    v_query := 'SELECT * FROM ' || p_table_name;
    
    v_file := UTL_FILE.FOPEN('BANK_DATA_DIR', p_file_name, 'W', 32767);
    
    FOR r IN (EXECUTE IMMEDIATE v_query) LOOP
        v_data := '';
        FOR i IN 1..r.COUNT LOOP
            v_data := v_data || '"' || REPLACE(r(i), '"', '""') || '",';
        END LOOP;
        UTL_FILE.PUT_LINE(v_file, RTRIM(v_data, ','));
    END LOOP;
    
    UTL_FILE.FCLOSE(v_file);
EXCEPTION
    WHEN OTHERS THEN
        UTL_FILE.FCLOSE(v_file);
        RAISE;
END;
-- 调用示例
BEGIN
    export_to_csv('BANK_ACCOUNTS', 'accounts.csv'); -- 导出账户表
    export_to_csv('TRANSACTIONS', 'transactions.csv'); -- 导出交易表
END;

--从CSV导入（使用外部表）
CREATE TABLE bank_accounts_ext (
    account_id   NUMBER,
    account_name VARCHAR2(50),
    balance      NUMBER,
    created_at   VARCHAR2(20) -- CSV中时间格式需统一
) ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY BANK_DATA_DIR
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        FIELDS TERMINATED BY ','
        MISSING FIELD VALUES ARE NULL
        (
            account_id, account_name, balance, created_at
        )
    )
    LOCATION ('accounts.csv')
);
commit;

-- 数据加载
INSERT INTO bank_accounts (account_id, account_name, balance, created_at)
SELECT account_id, account_name, balance, TO_TIMESTAMP(created_at, 'YYYY-MM-DD HH24:MI:SS')
FROM bank_accounts_ext;


-- 导出账户表
BEGIN
    DBMS_SQL.EXPORT_DATA(
        filename => 'accounts.csv',
        directory => 'BANK_DATA_DIR',
        query    => 'SELECT * FROM bank_accounts'
    );
END;

-- 导出交易表
BEGIN
    DBMS_SQL.EXPORT_DATA(
        filename => 'transactions.csv',
        directory => 'BANK_DATA_DIR',
        query    => 'SELECT * FROM transactions'
    );
END;


-- 测试1：创建账户
BEGIN
    create_account('Charlie', 2000.00); -- 创建账户（使用序列生成ID）
END;


--插入交易数据（TRANSACTIONS）



-- Charlie 尝试透支取款（失败场景，不插入数据，但记录错误日志）
-- 见存储过程逻辑中的错误处理



-- 测试2：存款
BEGIN
    deposit(1001, 500.00);
END;


-- 测试3：取款（透支测试）
BEGIN
    withdraw(1002, 4000.00); -- 应抛出错误
END;


-- 测试 并发转账（需在多个会话中执行）
BEGIN
    transfer(1001, 1002, 1000); -- Session 1
    transfer(1001, 1002, 1000); -- Session 2（应有一方失败）
END;

-- 测试2: CSV导出后重新导入验证
DECLARE
    v_count NUMBER;
BEGIN
    export_to_csv('BANK_ACCOUNTS', 'backup.csv');
    DELETE FROM bank_accounts;
    @import_script.sql; -- 执行导入脚本
    SELECT COUNT(*) INTO v_count FROM bank_accounts;
    IF v_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20003, 'CSV导入失败');
    END IF;
END;


-- 创建表与序列：执行上述CREATE TABLE和CREATE SEQUENCE语句。
-- 编译存储过程：在SQL*Plus或Oracle SQL Developer中运行PL/SQL代码。
-- 执行测试脚本：调用测试用例验证功能。
-- 导出/导入CSV：配置目录权限后执行导出操作。