<?php
header("Content-Type: text/html; charset=utf-8");
require(dirname(__FILE__) . '/includes/init.php');
require(dirname(__FILE__) . '/includes/ao/BaseAO.php');
require(dirname(__FILE__) . '/includes/ao/OrderAO.php');

//$subject="select * from a where id=10 limit 0 , 10  for update ";
//
//$pattern="/limit\s+([0-9]+)\s?(?:,\s?([0-9]+))?/";
//preg_match($pattern, $subject, $matches);
//if(count($matches)==3){
//    $start=$matches[1];
//    $end=$matches[2];
//}
//if(count($matches)==2){
//    $start=$matches[1];
//}
//print_r($matches);
//die;

//$a=cls_sqlexecute::getInstance('mmall_0000',$user_multiple_dbroute_config);
//$c=cls_sqlexecute::getInstance('mmall_0002',$user_multiple_dbroute_config);
//print_r($a->getList());
//$logic='user_id';
////$subject = "where user_id = #abccs#";
////$subject = "where user_id =#abccs#";
//$subject = "here id=#id# and user_id=#user_id#";
//$pattern = "/\s*$logic\b\s*=\s*/";
////$pattern = '/(\s*)+user_id+(\s*)+/';
//preg_match($pattern, $subject, $matches);
//print_r($matches);
//
//// /user_id\s+=\s#/;
//
//die;
//$array=array();
//for($i=0;$i<300000;$i++){
// $array[]='str_key_'.$i. "hello world,user_multiple_dbroute_config,user_multiple_dbroute_config,user_multiple_dbroute_config";
//}
//cls_shmop::writeArray('strkey',$array);
//echo "4444";
//die;
$ao = new OrderAO();
$ao->testTransaction(10);
//die;
//$order=new OrderModel();
//$id=$order->getAll(10);
//echo "insert<br>";
//print_r($id);

//die;
//$order_goods=new OrderGoodsModel();
//$id=$order_goods->insert();
//print_r($id);
//die();
//$m=new RefundInfoModel();
//print_r($m->insert(10,10));
//$order = new OrderModel();
//$result = $order->update(34084, 10);
//echo "update<br>";
//print_r($result);
//die;

$ao = new OrderAO();
//$a=new ConsistentHash($sc_refund_info_multiple_dbroute_config);
//$a->explodeString();
//
//for($i=0;$i<1024;$i++){
//	$db=$a->getDbName($i);
//	$t=$a->getTableName($i);
//	echo $i.'=>'.$db."=>$t<br>";
//}
//die;
$ao->testTransaction(10);
//die;

$order = new OrderModel();
print_r($order->transactionTest());
//die;
$id = $order->insert();
echo "insert<br>";
print_r($id);

$result = $order->update(34084, 10);
echo "update<br>";
print_r($result);


$cityModel = new CityModel();
//print_r($cityModel->getAllCity());
//die;
$mop = new cls_sequence();
//$num=$mop->nextValue('user');
//echo $num."<br>";
//die;

$id = $order->insert();
echo "insert<br>";
print_r($id);

print_r($order->transactionTest());

$res=$order->queryAllByIn();
print_r($res);

$res = $order->queryAll();
print_r($res);

$result = $order->getAll();
echo "getAll<br>";
print_r($result);

$res = $order->queryAll();
echo "queryAll<br>";
print_r($res);

$id = $order->insert();
echo "insert<br>";
print_r($id);

$result = $order->getRow(599);
echo "getRow<br>";
print_r($result);

$result = $order->getOne(1);
echo "getOne<br>";
print_r($result);

$result = $order->delete(583, 10);
echo "delete<br>";
print_r($result);

$result = $order->update(584, 10);
echo "update<br>";
print_r($result);

