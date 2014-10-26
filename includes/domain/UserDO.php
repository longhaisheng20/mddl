<?php
class UserDO{

	private $id;
	
	private $newUserName;
	
	private $userName;
	
	private $passWord;
	
	private $addressOne;
	
	private $isDelete;
	
	private $gmtCreate;
	
	private $gmtModified;

    public function setAddressOne($addressOne) {
        $this->addressOne = $addressOne;
    }

    public function getAddressOne() {
        return $this->addressOne;
    }

    public function setGmtCreate($gmtCreate) {
        $this->gmtCreate = $gmtCreate;
    }

    public function getGmtCreate() {
        return $this->gmtCreate;
    }

    public function setGmtModified($gmtModified) {
        $this->gmtModified = $gmtModified;
    }

    public function getGmtModified() {
        return $this->gmtModified;
    }

    public function setId($id) {
        $this->id = $id;
    }

    public function getId() {
        return $this->id;
    }

    public function setIsDelete($isDelete) {
        $this->isDelete = $isDelete;
    }

    public function getIsDelete() {
        return $this->isDelete;
    }

    public function setPassWord($passWord) {
        $this->passWord = $passWord;
    }

    public function getPassWord() {
        return $this->passWord;
    }

    public function setNewUserName($userName) {
        $this->newUserName = $userName;
    }

    public function getNewUserName() {
        return $this->newUserName;
    }
    public function setUserName($userName) {
        $this->userName = $userName;
    }

    public function getUserName() {
        return $this->userName;
    }

}