<?php
class OrmModel extends OracleTemplate{

	private $resultMap=array("brand_id"=>'id',"BRAND_NAME"=>'brandName',
							"brand_logo"=>'brandLogo',"BRAND_DESC"=>"brandDesc",
							"SITE_URL"=>'siteUrl',"SORT_ORDER"=>'sortOrder',
							"IS_SHOW"=>'isShow',"IS_DELETED"=>'isDeleted',
							"LATEST_TIME"=>'latestTime','class'=>'BrandDO');

	private $select_files=" brand_id,BRAND_NAME,brand_logo,BRAND_DESC,SITE_URL,SORT_ORDER,IS_SHOW,IS_DELETED,LATEST_TIME ";
	
	public function __construct() {
		parent::__construct();
	}
	
	public function init(){
		$this->setResultMap($this->resultMap);
	}

	public  function queryBrands(BrandDO $brand){
		$sql="select ".$this->select_files." from SC_BRAND where brand_id=#id# ";
		return $this->queryForList($sql, $brand);
	}
	
	public  function getBrandCount(BrandDO $brand){
		$sql="select count(brand_id) from SC_BRAND where brand_id>=#id# ";
		return $this->getColumn($sql, $brand);
	}

	public function getBrandById($brandId){
		$sql="select ".$this->select_files." from  SC_BRAND where brand_id=#id# ";
		return $this->queryForObject($sql,$brandId);
	}

	public function getBrandByWithArray(array $params=array()){
		$sql="select ".$this->select_files." from  SC_BRAND where brand_id=#brand_id#  and brand_id >10 ";
		return $this->queryForObject($sql, $params);
	}

	public function insert(BrandDO $brand){
		$sql="insert INTO SC_BRAND (brand_id,brand_name,brand_logo,brand_desc,site_url,sort_order,is_show,is_deleted)
		values(SEQ_SC_BRAND_ID.NEXTVAL,) #brandName#,#brandLogo#,#brandDesc#,#siteUrl#,#sortOrder#,#isShow#,#isDeleted# ";
		$this->save($sql, $brand);
	}

	public function DynamicUpdate(BrandDO $b){
		$sql=$this->BuildUpdate($b);
		$where=" where id>0 and ";
		$sql =$sql.$this->bulidWhere($b,$where);
		return $this->update($sql,$b);
	}

	public function updateById(BrandDO $b){
		$sql=$this->BuildUpdate($b);
		$where=" where id=#id# ";
		$sql =$sql.$where;
		return $this->update($sql,$b);
	}

	private function BuildUpdate(BrandDO $b){//取update语句 where 字符串前面的部分
		$build_update_sql="update SC_BRAND set ";

		$dynamic="";
		if(parent::isNotNull($b->getBrandName())){
			$dynamic .=",brand_name=#brandName#";
		}
		if(parent::isNotNull($b->getBrandLogo())){
			$dynamic .=",brand_logo=#brandLogo#";
		}
		if(parent::isNotNull($b->getBrandDesc())){
			$dynamic .=",brand_desc=#brandDesc#";
		}
		if(parent::isNotNull($b->getSiteUrl())){
			$dynamic .=",site_url=#siteUrl#";
		}
		if(parent::isNotNull($b->getSortOrder())){
			$dynamic .=",sort_order=#sortOrder#";
		}
		if(parent::isNotNull($b->getIsShow())){
			$dynamic .=",is_show=#isShow#";
		}
		if(parent::isNotNull($b->getIsDeleted())){
			$dynamic .=",is_deleted=#isDeleted#";
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
	private function bulidWhere(BrandDO $b,$where=''){
		$whereField="";
		if(parent::isNotNull($b->getBrandName())){
			$whereField .=" and brand_name=#brandName#";
		}
		if(parent::isNotNull($b->getBrandLogo())){
			$whereField .=" and brand_logo=#brandLogo#";
		}
		if(parent::isNotNull($b->getBrandDesc())){
			$whereField .=" and brand_desc=#brandDesc#";
		}
		if(parent::isNotNull($b->getSiteUrl())){
			$whereField .=" and site_url=#siteUrl#";
		}
		if(parent::isNotNull($b->getSortOrder())){
			$whereField .=" and sort_order=#sortOrder#";
		}
		if(parent::isNotNull($b->getIsShow())){
			$whereField .=" and is_show=#isShow#";
		}
		if(parent::isNotNull($b->getIsDeleted())){
			$whereField .=" and is_deleted=#isDeleted#";
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