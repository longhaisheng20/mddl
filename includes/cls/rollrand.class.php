<?php
class cls_rollrand {

    public static function get_db_host_roll($db_read_host_array, $db) {
        $key = md5(implode("_", array_keys($db_read_host_array) . implode(array_values($db_read_host_array))));
        $cache_array = cls_shmop::readArray($key);
        if ($cache_array) {
            $host = array_shift($cache_array);
            cls_shmop::writeArray($key, $cache_array);
        } else {
            $host_str = $db_read_host_array[$db];
            if ($host_str) {
                $host_array = explode(',', $host_str);
                $read_host_list = self::get_read_host_list($host_array);
                $host = array_shift($read_host_list);
                cls_shmop::writeArray($key, $read_host_list);
            }else{
                $host='';
            }
        }

        return $host;
    }

    public static function get_db_host_rand($db_read_host_array, $db) {
        $host_str = $db_read_host_array[$db];
        $host_array = explode(',', $host_str);
        $read_host_list = self::get_read_host_list($host_array);
        $num = rand(0, count($read_host_list) - 1);
        return $read_host_list[$num];
    }

    public static function get_write_db_host_rand($ips) {
        if(stripos($ips,',')){//双master
            $host_array = explode(',', $ips);
            $num = rand(0, count($host_array) - 1);
            return $host_array[$num];
        }
        return $ips;
    }

    /**
     *
     * 根据权重得到新host列表
     * @param array $list array("127.0.0.1_1",'192.168.1.101_2','127.168.2.1') 下划线后面的表示权重
     * @return array Array('127.0.0.1','192.168.1.101','192.168.1.101','127.168.2.1')
     */
    private static function get_read_host_list(array $list = array()) {
        if (empty($list) || !is_array($list)) return false;
        $host_list = array();
        foreach ($list as $value) {
            $new_list = explode('_', $value);
            if (count($new_list) > 1) {
                $count = $new_list[1];
                for ($i = 0; $i < $count; $i++) {
                    $host_list[] = $new_list[0];
                }
            } else {
                $host_list[] = $value;
            }
        }
        return $host_list;
    }
    
    public static function get_uuid(){
    	$time = explode(" ", microtime());
    	$time = $time [1] . ($time [0] * 1000);
    	return md5(date('YmdHis').$time.rand(1000,1999).rand(2000,2999).rand(3000,3999));
    }

    public static function get_rand_str(){
        return chr(rand(97,122)).chr(rand(97,122)).rand(1000,9999).chr(rand(97,122)).chr(rand(97,122));
    }
}