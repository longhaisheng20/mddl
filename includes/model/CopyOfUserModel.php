<?php
class UserModel extends MysqliTemplate{

	private $resultMap=array("id"=>'id',"user_name"=>'userName',
							"PASSWORD"=>'passWord',"address_one"=>"addressOne",
							"IS_DELETE"=>'isDelete',"gmt_create"=>'gmtCreate',"gmt_modified"=>'gmtModified','class'=>'UserDO');

	private $select_files=" id,user_name,password,address_one,IS_DELETE,gmt_create,gmt_modified ";

	public function __construct() {
		parent::__construct();
		parent::setResultMap($this->resultMap);
	}

	public  function getUsers(){
		$sql="select ".$this->select_files." from user  order by id desc";
		return $this->queryForList($sql);
	}
	public  function getOneUser(UserDO $user){
		$sql="select ".$this->select_files." from user where id=#id# ";
		return $this->queryForList($sql, $user);
	}

	public  function getUserCount(UserDO $user){
		$sql="select count(id) from user where id=#id# ";
		return $this->getColumn($sql, $user);
	}

	public function getUserById($id){
		$sql="select ".$this->select_files." from  user where id=#id# ";
		return $this->queryForObject($sql,$id);
	}

	/**
	 * 根据数组参数查询返回user对象
	 * @param array $params 参数key为id,pwd
	 */
	public function getUsersWithArray(array $params=array()){
		$sql="select ".$this->select_files." from  user where id=#ID#  and password=#pwd# and id >0 ";
		return $this->queryForObject($sql, $params);
	}

	public function insert(UserDO $user){
		$sql="insert INTO user (password,user_name,address_one,is_delete,gmt_create,gmt_modified)
		values(#passWord#,#userName#,#addressOne#,#isDelete#,now(),now() )";
		return $this->save($sql, $user);
	}

	/**
	 * 批量插入user
	 * @param array $params 数组中元素为 UserDO 对象
	 */
	public function batchInsert($params=array()){
		$sql="insert INTO user (password,user_name,address_one,is_delete,gmt_create,gmt_modified)
		values(#passWord#,#userName#,#addressOne#,#isDelete#,now(),now() )";
		return $this->batchExecute($sql, $params);
	}

	public function DynamicUpdate(UserDO $user){
		$sql=$this->BuildUpdate($user);
		$where=" where id>0 and ";
		$sql =$sql.$this->bulidWhere($user,$where);
		return $this->update($sql,$user);
	}

	public function updateById(UserDO $user){
		$sql=$this->BuildUpdate($user);
		$where=" where id=#id# ";
		$sql =$sql.$where;
		return $this->update($sql,$user);
	}

	private function BuildUpdate(UserDO $b){//取update语句 where 字符串前面的部分
		$build_update_sql="update user set ";

		$dynamic="";
		if(parent::isNotNull($b->getNewUserName())){
			$dynamic .=",user_name=#newUserName#";
		}
		if(parent::isNotNull($b->getPassWord())){
			$dynamic .=",password=#passWord#";
		}
		if(parent::isNotNull($b->getAddressOne())){
			$dynamic .=",address_one=#addressOne#";
		}
		if(parent::isNotNull($b->getIsDelete())){
			$dynamic .=",is_delete=#isDelete#";
		}
		if(parent::isNotNull($b->getGmtCreate())){
			$dynamic .=",gmt_create=#gmtCreate#";
		}
		if(parent::isNotNull($b->getGmtModified())){
			$dynamic .=",gmt_modified=#gmtModified#";
		}

		if($dynamic){
			$dynamic=trim(trim($dynamic),",");
			$build_update_sql =$build_update_sql." ".$dynamic;
		}

		return $build_update_sql;
	}

	/**
	 * @param BrandDO $b
	 * @param string $where 可以包含 'where'字符串，可以以'and' | 'or' 结尾
	 * @return string
	 */
	private function bulidWhere(UserDO $b,$where=''){
		$whereField="";
		if(parent::isNotNull($b->getUserName())){
			$whereField .=" and user_name=#userName#";
		}
		if(parent::isNotNull($b->getPassWord())){
			$whereField .=" and password=#passWord#";
		}
		if(parent::isNotNull($b->getAddressOne())){
			$whereField .=" and address_one=#addressOne#";
		}
		if(parent::isNotNull($b->getIsDelete())){
			$whereField .=" and is_delete=#isDelete#";
		}
		if(parent::isNotNull($b->getGmtCreate())){
			$whereField .=" and gmt_create=#gmtCreate#";
		}
		if(parent::isNotNull($b->getGmtModified())){
			$whereField .=" and gmt_modified=#gmtModified#";
		}

		$has_where=false;
		if(stristr($where, "where")){
			$has_where=true;
		}
		if(strlen(trim($where))>5){
			$where=strtolower($where);
			$where=trim(trim($where)," or");
			$where=trim(trim($where)," and");
		}
		if(strlen(trim($where))<=0){
			$whereField=trim(trim($whereField),"or");
			$whereField=trim(trim($whereField),"and");
		}

		$where=" ".$where.$whereField;
		if(!$has_where){
			$where =" where ".$where;
		}
		return $where;
	}

}