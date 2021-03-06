<?php
class cls_pdosqlexecute implements cls_idb {
	
	/** 主库连接 */
    private $connection;

	/** 从库连接 */
    private $read_connection;

	/** 数据库连接数组 */
    private $connect_array;

	/** 是否有读库*/
    private $has_read_db = false;

    /** 此次操作是否有事务 */
    private $this_operation_have_transaction = false;

    /** 存储数据库连接单例类数组,key为DB名称 */
    private static $single_instance_list = array();

    /** 一次事务中所包含的数据库名 */
    private static $db_name_list_in_one_transaction = array();

    /** 是否需要标记  一次事务中所包含的数据库名 默认为false,不标记,只有事务中代码才需要标记*/
    private static $need_record_db_name_in_one_transaction=false;

    /** 是否已执行xa_end方法 */
    private $has_execute_xa_end=false;

    /**
     * @param string $db_name  数据库名
     * @param ay $db_route_config 分库分表配置
     */
    private function __construct($db_route_config = array(),$db_name = '') {
        if (!$this->connect_array) {
            global $default_config_array;
            $this->connect_array = $db_route_config ? $db_route_config : $default_config_array;
            if ($db_name) {
                $this->connect_array['db'] = $db_name;
            }
        }
        if (!$this->has_read_db) {
            $this->has_read_db = isset($this->connect_array['read_db_hosts']);
        }
    }

    public static function getInstance($db_route_config = array(),$db_name = '') {
        global $default_config_array;
        if (empty($db_name)) {
            $db_name = $default_config_array['db'];
        }
        if (isset(self::$single_instance_list[$db_name])) {
            return self::$single_instance_list[$db_name];
        } else {
            self::$single_instance_list[$db_name] = new self($db_route_config,$db_name);
            return self::$single_instance_list[$db_name];
        }
    }

    /** 获取一次事务中所包含的所有数据库名*/
    public static function get_database_name_list_in_one_transaction(){
        return array_unique(self::$db_name_list_in_one_transaction);
    }

    public function getAll($sql, $params = array()) {
        $stmt = $this->query($sql, $params);
        $result = $stmt->fetchAll();
        $stmt->closeCursor();
        return $result;
    }

    private function query($sql, $params = array()) {
        $condition = $this->format($sql, $params);
        $stmt = $this->prepare($condition['sql']);
        try {
            $stmt->execute($condition['params']);
            return $stmt;
        } catch (PDOException $e) {
            if ($stmt) {
                $stmt->closeCursor();
            }
            throw new Exception('Error in : ' . $e->getMessage());
            return null;
        }
    }

    private function format($sql, $params = array()) {
        $sql = $this->formatSql(strtolower($sql));
        $params = $this->formatParams($params);
        $condition = array('sql' => $sql, 'params' => $params);

        return $condition;
    }

    private function formatSql($sql) {
        return preg_replace('/#(\w+)#/', ':$1', $sql);
    }

    private function formatParams($params = array()) {
        $result = array();
        foreach ($params as $k => $v) {
            $result[':' . strtolower($k)] = $v;
        }
        $params = $result;
        return $params;
    }

    private function prepare($sql) {
        $transaction_read_master = false; //transaction select is read from master
        if (defined('TRANSACTION_READ_MASTER')) {
            $transaction_read_master = TRANSACTION_READ_MASTER;
        }
        if (($this->this_operation_have_transaction && $transaction_read_master) || (substr(trim($sql),0,10)==='/*master*/') && stristr($sql, 'select ')) {
            $db = $this->getMasterConnection();
        } else {
            if ($this->has_read_db && preg_match('/^select\s/i', trim($sql))) {
                $db = $this->getReadConnection();
            } else {
                $db = $this->getMasterConnection();
            }
        }
        $stmt = $db->prepare($sql);
        if(self::$need_record_db_name_in_one_transaction){
            self::$db_name_list_in_one_transaction[]=$this->connect_array['db'];
        }
        return $stmt;
    }

    private function getReadConnection() {
        if (!$this->read_connection) {
            $connect_array = $this->connect_array; // load config db array
            $db_name = $this->connect_array['db'];
            $db_read_host_array = isset($this->connect_array['read_db_hosts']) ? $this->connect_array['read_db_hosts'] : array();
            $host=null;
            if(isset($connect_array['read_db_arithmetic']) && $connect_array['read_db_arithmetic']=='roll'){//poll
               $host = cls_rollrand::get_db_host_roll($db_read_host_array, $db_name);
            }else{
               $host = cls_rollrand::get_db_host_rand($db_read_host_array, $db_name);//rand
            }
            if(empty($host)){//if slave not exists,search in masters
                $db_host_array = isset($connect_array['db_hosts']) ? $connect_array['db_hosts'] : array();
                if ($db_host_array) {
                    $host = $db_host_array[$db_name];
                    if(stripos($host,',')){//double master
                        $host = cls_rollrand::get_write_db_host_rand($host);
                    }
                } else {
                    $host = $connect_array['host'];
                }
            }
            $connect_array['host'] = $host;
            return $this->read_connection = $this->getDbConnection($connect_array);
        }

        return $this->read_connection;
    }

    private function getMasterConnection() {
        if (!$this->connection) {
            $connect_array = $this->connect_array;
            $db_host_array = isset($connect_array['db_hosts']) ? $connect_array['db_hosts'] : array();
            if ($db_host_array) {
                $db = $connect_array['db'];
                $host = $db_host_array[$db];
                if(stripos($host,',')){//double master
                    $host = cls_rollrand::get_write_db_host_rand($host);
                }
            } else {
                $host = $connect_array['host'];
            }
            $connect_array['host']=$host;
            return $this->connection = $this->getDbConnection($connect_array);
        }
        return $this->connection;
    }

    private function getDbConnection($connect_array) {
        $dsn_array = array(
            'dbname=' . $connect_array['db'],
            'host=' . $connect_array['host'],
            'port=' . $connect_array['port'],
        );
        $dsn = 'mysql:' . implode(';', $dsn_array);
        try {
            $connection = new PDO($dsn, $connect_array['user_name'], $connect_array['pass_word'], array(
                    PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES 'UTF8'",
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                    PDO::ATTR_CASE => PDO::CASE_LOWER,
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_PERSISTENT => false,//not persistence
                )
            );
            return $connection;
        } catch (PDOException $e) {
            throw new Exception('Database Connect Error : ' . $e->getMessage());
        }
    }

    public function begin() {
        $this->getMasterConnection();
        self::$need_record_db_name_in_one_transaction=true;
        if(self::$need_record_db_name_in_one_transaction){
            self::$db_name_list_in_one_transaction[]=$this->connect_array['db'];
        }
        $this->connection->setAttribute(PDO::ATTR_AUTOCOMMIT, false);
        $this->this_operation_have_transaction = true;
        return $this->connection->beginTransaction();
    }

    public function commit() {
        if(self::$need_record_db_name_in_one_transaction){
            self::$db_name_list_in_one_transaction[]=$this->connect_array['db'];
        } 	
        if(count(self::get_database_name_list_in_one_transaction())>1){
            throw new Exception(' transactions have more than one database,plese check you code ');
        }
        $return = $this->connection->commit();
        if(self::$need_record_db_name_in_one_transaction){
            self::$db_name_list_in_one_transaction=array();
            self::$need_record_db_name_in_one_transaction=false;
        }
        $this->connection->setAttribute(PDO::ATTR_AUTOCOMMIT, true);
        $this->this_operation_have_transaction = false;
        return $return;
    }

    public function rollBack() {
        $return = $this->connection->rollBack();
        if(self::$need_record_db_name_in_one_transaction){
            self::$db_name_list_in_one_transaction=array();
            self::$need_record_db_name_in_one_transaction=false;
        }
        $this->connection->setAttribute(PDO::ATTR_AUTOCOMMIT, true);
        $this->this_operation_have_transaction = false;
        return $return;
    }

    /**
     * 插入数据
     * @param $sql
     * @param array $params
     * @param bool $return_insert_id
     * @return int
     */
    public function insert($sql, $params = array(), $return_insert_id = true) {
        $stmt = $this->query($sql, $params);
        $stmt->closeCursor();
        $insertId = $return_insert_id ? $stmt->lastInsertId : 0;//在dbroute使用自己生成主键时，此处会抛出一个警告，无法获取到上一次插入的ID
        return $insertId;
    }

    /**
     * @param $sql "update order set order_num=#order_num#,order_sn=#order_sn# where id=#id# and user_id=#user_id#"
     * @param array $params array('order_num'=>3,'order_sn'=>'sn123456','id'=>1,'user_id'=>10)
     * @param bool $return_affected_rows
     * @return int
     */
    public function update($sql, $params = array(), $return_affected_rows = true) {
        $stmt = $this->query($sql, $params);
        $rowCount = $return_affected_rows ? $stmt->rowCount() : 0;
        $stmt->closeCursor();

        return $rowCount;
    }

    /**
     * @param $sql "delete from order where id=#id# and user_id=#user_id#"
     * @param array $params array('id'=>123,'user_id'=>10)
     * @param bool $return_affected_rows
     * @return int
     */
    public function delete($sql, $params = array(), $return_affected_rows = true) {
        return $this->update($sql, $params, $return_affected_rows);
    }

    /**
     * @param $sql "insert order(order_id,order_sn,user_id) values (#order_id#,#order_sn#,#user_id#)"
     * @param array $batch_params (array(array('order_id'=>1,'order_sn'=>'password1','user_id'=>10),array('order_id'=>2,'order_sn'=>'password2','user_id'=>10)......))
     * @param int $batch_num 不见意超过50,默认为20
     * @internal param array $logic_params 分表物理列名数组，如根据user_id分表的，此可传 array('user_id'=>10)
     * @return int
     */
    public function batchExecutes($sql, $batch_params = array(), $batch_num = 20) {
        if (empty($batch_params)) return;
        $affectedRows = 0;
        $sql = $this->formatSql($sql);
        array_walk($batch_params, array($this, 'formatParams'));
        $stmt = $this->prepare($sql);

        $paramsGroups = array_chunk($batch_params, $batch_num);
        foreach ($paramsGroups as $group) {
            $this->begin();
            foreach ($group as $params) {
                $stmt->execute($params);
                $affectedRows += $stmt->rowCount();
            }
            $this->commit();
            $stmt->closeCursor();
        }

        return $affectedRows;
    }

    /**
     * @param $sql "select  order_id,order_sn from order where user_id=#user_id# "
     * @param array $params
     * @internal param array $bind_params array('user_id'=>10)
     * @return array
     */
    public function getRow($sql, $params = array()) {
        $stmt = $this->query($sql, $params);
        $result = $stmt->fetch();
        $stmt->closeCursor();

        return $result;
    }

    /**
     * @param $sql "select count(1) as count_num from order where user_id=#user_id# "
     * @param array $params
     * @internal param array $bind_params array('user_id'=>100)
     * @return int
     * @see getColumn
     */
    public function getOne($sql, $params = array()) {
        return $this->getColumn($sql, $params);
    }

    /**
     * @param $sql "select count(1) as count_num from order where user_id=#user_id# "
     * @param array $params
     * @internal param array $bind_params array('user_id'=>100)
     * @return int
     */
    public function getColumn($sql, $params = array()) {
        $stmt = $this->query($sql, $params);
        $column = $stmt->fetchColumn();
        $stmt->closeCursor();

        return $column;
    }

    public function closeConnection() {
        $this->connection = null;
        $this->read_connection = null;
    }

    public function xa_start($uuid){
    	$this->getMasterConnection();
    	$this->connection->query("XA START '$uuid'");
    }
    
    public function xa_end_prepare($uuid){
    	$this->connection->query("XA END '$uuid'");
        $this->has_execute_xa_end=true;
		$this->connection->query("XA PREPARE '$uuid'");
    }
    
    public function xa_commit($uuid){
		$this->connection->query("XA COMMIT '$uuid'");
    }
    
    public function xa_rollback($uuid){
        if(!$this->has_execute_xa_end){
            try{
                $this->connection->query("XA END '$uuid'");
                $this->has_execute_xa_end=false;
            }catch(PDOException $e){
                
            }
        }
		$this->connection->query("XA ROLLBACK '$uuid'");
    }
    
    public function __destruct() {
        $this->closeConnection();
    }
}