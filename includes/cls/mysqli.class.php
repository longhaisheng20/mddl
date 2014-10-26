<?php
/**
 * mysqli 操作类
 * @author longhaisheng(longhaisheng20@163.com,QQ:87188524)
 */
class cls_mysqli {

	public $connection;

	public $connect_array=array();

	public function __construct() {
		if(empty($this->connect_array)){
			$this->connect_array['host']=DB_HOST;
			$this->connect_array['user']=DB_USER_NAME;
			$this->connect_array['pass']=DB_PASSWORD;
			$this->connect_array['db']=DB_NAME;
			$this->connect_array['port']=DB_PORT;
		}
	}

	private function init(){
		if ($this->connection === null) {
			$connect_array=$this->connect_array;
			$this->connection = new mysqli($connect_array['host'], $connect_array['user'], $connect_array['pass'], $connect_array['db'],$connect_array['port']);
			if (mysqli_connect_errno()) {
				echo("Database Connect Error : " . mysqli_connect_error($this->connection));
			} else {
				$this->connection->query("SET NAMES 'utf8'");
			}
		}
	}

	private function getConnection() {
		return $this->connection;
	}

	/**
	 * @param $sql "insert user (name,pwd) values (?,?) "
	 * @param array $params array('long','123456')
	 * @param bool $return_insert_id
	 * @return int
	 */
	public function insert($sql, $params = array(), $return_insert_id = true) {
		$stmt = $this->executeQuery($sql, $params);
		if ($stmt && $return_insert_id) {
			$insert_id=$stmt->insert_id;
			$stmt->close();
			return $insert_id;
		}
		if($stmt!=null){
			$stmt->close();
		}

	}

	/**
	 * @param $sql "update user set name=?,pwd=? where id=?"
	 * @param array $params array('longhaisheng','pwd123456',1)
	 * @param bool $return_affected_rows
	 * @return int
	 */
	public function update($sql, $params = array(), $return_affected_rows = true) {
		$stmt = $this->executeQuery($sql, $params);
		if ($stmt && $return_affected_rows) {
			$affected_rows=$stmt->affected_rows;
			$stmt->close();
			return $affected_rows;
		}
		if($stmt!=null){
			$stmt->close();
		}
	}

	/**
	 * @param $sql "delete from user where id=?"
	 * @param array $params array(123)
	 * @param bool $return_affected_rows
	 * @return int
	 */
	public function delete($sql, $params = array(), $return_affected_rows = true) {
		return $this->update($sql, $params, $return_affected_rows);
	}

	/**
	 * @param $sql "insert user(name,pwd) values (?,?)"
	 * @param array $batch_params (array(array('username1','password1'),array('username2','password2')......))
	 * @param int $batch_num 不见意超过50,默认为20
	 * @return 总共受影响行数
	 */
	public function batchExecutes($sql, $batch_params = array(), $batch_num = 20) {
		$affected_rows=0;
		if ($batch_params && is_array($batch_params)) {
			$this->init();
			$stmt = $this->connection->prepare($sql);
			$count = count($batch_params);
			$i = 0;
			foreach ($batch_params as $param) {
				$i++;
				if ($i % $batch_num == 0 || $i = $count) {
					$this->begin();
				}
				$params = $this->get_bind_params($param);
				$this->bindParameters($stmt, $params);
				$stmt->execute();
				if ($i % $batch_num == 0 || $i = $count) {
					$this->commit();
					$affected_rows= $affected_rows + $stmt->affected_rows;
				}
			}
			if($stmt != null){
				$stmt->close();
			}
			if($this->connection != null){
				$this->connection->autocommit(true);
			}
			return $affected_rows;
		}
	}

	/**
	 * @param $sql "select id,name,pwd from user where id >?"
	 * @param array $bind_params array(10)
	 * @return array
	 */
	public function getAll($sql, $params = array()) {
		$stmt = $this->executeQuery($sql, $params);
		$fields_list = $this->fetchFields($stmt);

		foreach ($fields_list as $field) {
			$bind_result[] = &${$field};//http://www.php.net/manual/zh/language.variables.variable.php
		}
		$this->bindResult($stmt, $bind_result);
		$result_list = array();
		$i = 0;
		while ($stmt->fetch()) {//http://cn2.php.net/manual/zh/mysqli-stmt.bind-result.php
			foreach ($fields_list as $field) {
				$result_list[$i][$field] = ${$field};
			}
			$i++;
		}
		if($stmt!=null){
			$stmt->close();
		}
		return $result_list;
	}

	/**
	 * @param $sql "select id,name,pwd from user where id=? "
	 * @param array $bind_params array(10)
	 * @return array
	 */
	public function getRow($sql, $params = array()) {
		$list = $this->getAll($sql, $params);
		if ($list) {
			return $list[0];
		}
		return array();
	}

	/**
	 * @param $sql "select count(1) as count_num from user where id >? "
	 * @param array $bind_params array(100)
	 * @return int
	 * @see getColumn
	 */
	public function getOne($sql, $params = array()) {
		return $this->getColumn($sql, $params);
	}

	/**
	 * @param $sql "select count(1) as count_num from user where id >? "
	 * @param array $bind_params array(100)
	 * @return int
	 */
	public function getColumn($sql, $params = array()) {
		$row = $this->getRow($sql, $params);
		if ($row) {
			sort($row);
			return $row[0];
		}
		return 0;
	}

	private function executeQuery($sql, $params = array()) {
		$this->init();
		$stmt = $this->connection->prepare($sql);
		$params = $this->get_bind_params($params);
		$this->bindParameters($stmt, $params);

		if ($stmt->execute()) {
			return $stmt;
		} else {
			echo("Error in : " . mysqli_error($this->connection));
			if($stmt!=null){
				$stmt->close();
			}
			return 0;
		}
	}

	private function get_bind_params($bind_params) {
		if ($bind_params && is_array($bind_params)) {
			ksort($bind_params);
			$param_key = "";
			foreach ($bind_params as $key => $value) {
				$type = gettype($value);
				if ($type === "integer") {
					$param_key .= "i";
				} else if ($type === "double") {
					$param_key .= "d";
				} else if ($type === "string") {
					$param_key .= "s";
				} else {
					$param_key .= "b";
				}
			}
			array_unshift($bind_params, $param_key); //在数组最前面插入一条数据
			return $bind_params;
		}
		return array();
	}

	private function bindParameters($stmt, $bind_params = array()) {
		if ($bind_params) {
			call_user_func_array(array($stmt, "bind_param"), $this->refValues($bind_params));
		}
	}

	private function bindResult($stmt, $bind_result_fields = array()) {
		call_user_func_array(array($stmt, "bind_result"), $bind_result_fields);
	}

	private function refValues($arr){
		if (strnatcmp(phpversion(),'5.3') >= 0){ //Reference is required for PHP 5.3+
			$refs = array();
			foreach($arr as $key => $value){
				$refs[$key] = &$arr[$key];
			}
			return $refs;
		}
		return $arr;
	}

	private function fetchFields($stmt) {
		$metadata = $stmt->result_metadata();
		$field_list = array();
		while ($field = $metadata->fetch_field()) {
			$field_list[] = strtolower($field->name);
		}
		return $field_list;
	}

	public function begin() {
		$this->init();
		$this->connection->autocommit(false);//关闭本次数据库连接的自动命令提交事务模式
	}

	public function commit() {
		$this->connection->commit();//提交事务后，打开本次数据库连接的自动命令提交事务模式
		$this->connection->autocommit(true);
	}

	public function rollBack() {
		$this->connection->rollback();//回滚事务后，打开本次数据库连接的自动命令提交事务模式
		$this->connection->autocommit(true);
	}

	public function closeConnection(){
		if ($this->connection != null) {
			$this->connection->close();
			$this->connection = null;
		}
	}

	public function __destruct() {
		$this->closeConnection();
	}
}

?>

