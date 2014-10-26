<?php
require(dirname(__FILE__) . '/includes/init.php');
define("MYSQL_XA_EXTEND", 'mysqli');//mysql_pdo

echo "========";
if (MYSQL_XA_EXTEND === 'mysqli') {
    mysqli_report(MYSQLI_REPORT_ALL);
    $mmall = new mysqli("127.0.0.1", "root", "123456", "mmall", 3307)or die("$mmall ： 连接失败");
    $sooch = new mysqli("127.0.0.1", "root", "123456", "sooch", 3307)or die("$sooch ： 连接失败");
    $mmall->set_charset("utf8");
    $sooch->set_charset("utf8");
} else {
    $dsn_array = array(
        'dbname=mmall',
        'host=127.0.0.1',
        'port=3307',
    );
    $dsn = 'mysql:' . implode(';', $dsn_array);
    $mmall = new PDO($dsn, "root", "123456", array(
            PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES 'UTF8'",
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_CASE => PDO::CASE_LOWER,
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_PERSISTENT => false, //非持久化连接
        )
    );
    $dsn_array = array(
        'dbname=sooch',
        'host=127.0.0.1',
        'port=3307',
    );
    $dsn = 'mysql:' . implode(';', $dsn_array);
    $sooch = new PDO($dsn, "root", "123456", array(
            PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES 'UTF8'",
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_CASE => PDO::CASE_LOWER,
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_PERSISTENT => false, //非持久化连接
        )
    );
}
$uuid = cls_rollrand::get_uuid();
$sooch_grid = $uuid . ',' . cls_rollrand::get_rand_str(); //xid: gtrid [, bqual [, formatID ]] gtrid is a global transaction identifier, bqual is a branch qualifier
$mmall_grid = $uuid . ',' . cls_rollrand::get_rand_str();
$mmall_grid_1 = $uuid . ',' . cls_rollrand::get_rand_str();
$sooch->query("XA START '$sooch_grid'"); //准备事务1
$mmall->query("XA START '$mmall_grid'"); //准备事务2
//$mmall->query("XA START '$mmall_grid_1'"); //准备事务2

try {
    $sql1 = "insert into city(city_name,city_code,parent_id,type,gmt_created,gmt_modified) value ('北京','bj',0,1,now(),now()) ";
    $stmt = $sooch->prepare($sql1);
    if (!$stmt) {
        throw new Exception('error sql in ' . $sql1);
    }
    $return = $stmt->execute();
    if ($return == false) {
        throw new Exception('error sql in ' . $sql1);
    }

    $sql2 = "insert into city(city_name7,city_code,parent_id,type,gmt_created,gmt_modified) value ('上海','sh',0,1,now(),now()) ";
    $stmt = $mmall->prepare($sql2);
    if (!$stmt) {
        throw new Exception('error sql in ' . $sql2);
    }
    $return = $stmt->execute();
    if ($return == false) {
        throw new Exception("mmall update error!");
    }

    $sooch->query("XA END '$sooch_grid'");
    $sooch->query("XA PREPARE '$sooch_grid'");
    $mmall->query("XA END '$mmall_grid'");
    $mmall->query("XA PREPARE '$mmall_grid'");

    $mmall->query("XA COMMIT '$mmall_grid'");
    $sooch->query("XA COMMIT '$sooch_grid'");

} catch (Exception $e) { //PDO xa end 需捕获异常 mysqli 不用
    if (MYSQL_XA_EXTEND === 'mysqli') {
        try{
            $sooch->query("XA END '$sooch_grid'");
            $mmall->query("XA END '$mmall_grid'");
            $sooch->query("XA END '$sooch_grid'");
            $mmall->query("XA END '$mmall_grid'");
        }catch(mysqli_sql_exception $e ){

        }
        $mmall->query("XA ROLLBACK '$mmall_grid'");
        $sooch->query("XA ROLLBACK '$sooch_grid'");
    } else {
        try {
            $sooch->query("XA END '$sooch_grid'");
            $sooch->query("XA END '$sooch_grid'");
        } catch (PDOException $p) {
            echo " end exception for " . $p->getMessage();
        }
        try {
            $mmall->query("XA END '$mmall_grid'");
            $mmall->query("XA END '$mmall_grid'");
        } catch (PDOException $p) {
            echo " end exception for " . $p->getMessage();
        }
        $mmall->query("XA ROLLBACK '$mmall_grid'");
        $sooch->query("XA ROLLBACK '$sooch_grid'");
    }


    print $e->getMessage();
}
if (MYSQL_XA_EXTEND === 'mysqli') {
    $sooch->close();
    $mmall->close();
}
echo "=====";
die;

//$sooch->query("XA START '$grid'");
//
//$sql = "SELECT * FROM test_transation2 WHERE id=2";
//$result = $sooch->query($sql) or die("查询失败");
//echo "<pre>";
//print_r(mysqli_fetch_assoc($result));
//echo "</pre>";
//
//$sooch->query("XA END '$grid'");
//$sooch->query("XA PREPARE '$grid'");
//
//$sooch->query("XA COMMIT '$grid'");
//
//
//$mmall->query("XA start $grid");
//$sql = "insert into test_transation1 values(4,'小虎')";
//
//$result = $mmall->query($sql);
//$sql = "select * from test_transation1";
//$result = $mmall->query($sql) or die("查询失败");
//echo "<pre>";
//print_r(mysqli_fetch_assoc($result));
//echo "</pre>";
//$mmall->query("XA END $grid");
//$mmall->query("XA prepare $grid");
//$mmall->query("XA commit $grid");
//
//$sooch->close();
//$mmall->close();

?>

