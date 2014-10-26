<?php
class BaseAO {

    private $xa_distribute_dbroutes = array(); //二维数组 key为uuid value为dbroute数组

    private $xa_db_names = array(); //二维数组 key为uuid value为db_name数组

    public function __construct() {

    }

    public function addDistributeDbroute($dbroute, $uuid, $xa_params = array()) {
        if ($dbroute == null) return false;
        if (empty($uuid)) return false;
        $db_name = $dbroute->getDateBaseName($xa_params);
        $xa_db_names = $this->xa_db_names[$uuid];
        if ($xa_db_names && is_array($xa_db_names) && in_array($db_name, $xa_db_names)) { //一个库的xid只能执行一次xa start 否则是报错
            return false;
        } else {
            $this->xa_db_names[$uuid][] = $db_name;
            $distributed_dbroutedo = new DistributeDbrouteDO();
            $distributed_dbroutedo->setDbroute($dbroute);
            $distributed_dbroutedo->setXid($uuid . "," . cls_rollrand::get_rand_str()); //http://dev.mysql.com/doc/refman/5.6/en/xa-statements.html
            $distributed_dbroutedo->setXaParams($xa_params);
            $this->xa_distribute_dbroutes[$uuid][] = $distributed_dbroutedo;
            return true;
        }
    }

    public function xa_start($uuid) {
        if ($this->xa_distribute_dbroutes[$uuid]) {
            foreach ($this->xa_distribute_dbroutes[$uuid] as $distribute) {
                $distribute->getDbroute()->xa_start($distribute->getXid(), $distribute->getXaParams());
            }
        }
    }

    private function xa_end_prepare($uuid) {
        if ($this->xa_distribute_dbroutes[$uuid]) {
            foreach ($this->xa_distribute_dbroutes[$uuid] as $distribute) {
                $distribute->getDbroute()->xa_end_prepare($distribute->getXid(), $distribute->getXaParams());
            }
        }
    }

    private function xa_commit($uuid) {
        if ($this->xa_distribute_dbroutes[$uuid]) {
            foreach ($this->xa_distribute_dbroutes[$uuid] as $distribute) {
                $distribute->getDbroute()->xa_commit($distribute->getXid(), $distribute->getXaParams());
            }
        }
    }

    public function xa_end_prepare_and_commit($uuid) {
        $this->xa_end_prepare($uuid);
        $this->xa_commit($uuid);
        $this->xa_distribute_dbroutes[$uuid] = array();
        $this->xa_db_names[$uuid] = array();
    }

    public function xa_rollback($uuid) {
        if ($this->xa_distribute_dbroutes[$uuid]) {
            foreach ($this->xa_distribute_dbroutes[$uuid] as $distribute) {
                $distribute->getDbroute()->xa_rollback($distribute->getXid(), $distribute->getXaParams());
            }
            $this->xa_distribute_dbroutes[$uuid] = array();
            $this->xa_db_names[$uuid] = array();
        }
    }

}

class DistributeDbrouteDO {

    private $xid;

    private $dbroute;

    private $xa_params;

    public function setDbroute($dbroute) {
        $this->dbroute = $dbroute;
    }

    public function getDbroute() {
        return $this->dbroute;
    }

    public function setXid($uuid) {
        $this->xid = $uuid;
    }

    public function getXid() {
        return $this->xid;
    }

    public function setXaParams($xa_params) {
        $this->xa_params = $xa_params;
    }

    public function getXaParams() {
        return $this->xa_params;
    }

}