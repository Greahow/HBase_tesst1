package com.blueasdfg;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.hbase.client.Durability.SYNC_WAL;

/**
 * Hello world!
 *
 */
public class App 
{
    //创建Hadoop以及HBase管理配置对象
    public static Configuration config;
    public static Connection connection;
    public static Admin admin;

    static {
        // 使用HBaseConfiguration的单例方法实例化
        config = HBaseConfiguration.create();

    }

    //初始化
    public void init() throws IOException {

        //创建连接到HBase数据库的Connection对象
        connection = ConnectionFactory.createConnection(config);

        //通过connection对象获取Admin对象，它负责实现表的操作
        admin = connection.getAdmin();
    }

    //关闭
    public void close() {
        try {
            if (admin != null)
                admin.close();
            if (connection != null)
                connection.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tablename
     * @return
     * @throws IOException
     */
    public boolean isTableExist(String tablename) throws IOException {
        init();
        boolean result =  admin.tableExists(TableName.valueOf(tablename));
        close();
        return result;
    }



    /**
     * 创建一个表
     *
     * @param table_name 表名
     * @param family_names 列簇名(数组)
     * @throws IOException
     */
    public void create_talbe(String table_name, String[] family_names) throws IOException{

        init();

        if (!admin.tableExists(TableName.valueOf(table_name))){

            // 获取TableName
            TableName tableName = TableName.valueOf(table_name);

            // table 描述
            HTableDescriptor htabledes = new HTableDescriptor(tableName);

            for (String family_name : family_names) {
                // column 描述
                HColumnDescriptor family = new HColumnDescriptor(family_name);
                htabledes.addFamily(family);
            }
            admin.createTable(htabledes);
        }else {
            System.out.println(table_name+"已存在");
        }

        close();
    }


    /**
     * 增加一行的记录
     *
     * @param table_name  表名称。。
     * @param row  行主键，用|拼接
     * @param columnFamily  列族名称
     * @param column  列名(可以为null)
     * @param value 值
     * @param num 数组的长度和columnFamily一样，每个元素代表对应的columnFamily有几个column
     * @throws IOException
     *
     * 调用方法示例:addData_One(test1,2,[one,two],[one1,one2,two1,two2,two3],[1,2,3,4,5],[2,3])
     */
    public void addData_One(String table_name, String row, String[] columnFamily, String[] column,
                            String[] value, int[] num) throws IOException{
        init();
        // 表名对象
        TableName tableName = TableName.valueOf(table_name);
        //表对象
        Table table = connection.getTable(tableName);
        // put对象 负责录入数据
        Put put = new Put(row.getBytes("utf8")); //指定行   和   编码格式

        //count用于先定下次column和value的遍历从哪里开始,因为参数传过来是数组的形式
        int count = 0;

        //外层遍历columnFamily
        for (int i= 0; i< columnFamily.length; i++){
            //内层遍历column
            for (int j = count; j < count+num[i]; j++){
                put.addColumn(
                        columnFamily[i].getBytes("utf8"),
                        column[j].getBytes("utf8"),
                        value[j].getBytes("utf8")
                );
            }
            count = count + num[i];
        }

        /*
         * 设置写WAL（Write-Ahead-Log）的级别 参数是一个枚举值，可以有以下几种选择：
         * ASYNC_WAL ：当数据变动时，异步写WAL日志，数据量巨大时，有可能丢失
         * SYNC_WAL ： 当数据变动时，同步写WAL日志,确保不会丢失
         * FSYNC_WAL ： 当数据变动时，同步写WAL日志，并且，强制将数据写入磁盘
         * SKIP_WAL ： 不写WAL日志
         * USE_DEFAULT ： 使用HBase全局默认的WAL写入级别，即SYNC_WAL
         */
        put.setDurability(SYNC_WAL);
        table.put(put);
        table.close();
        close();
    }


    /**
     * 批量插入
     *
     * @param table_name  表名
     * @param row  行主键
     * @param columnFamily  列簇，表中共有几个列簇
     * @param column  列名，表中共有几个列
     * @param value  值，这个数组的长度=row的长度*column的长度，顺序是按照行从左到右依次填入
     * @param num  数组的长度和columnFamily一样，每个元素代表对应的columnFamily有几个column
     * @throws IOException
     */
    public void addData_Multi(String table_name, String[] row, String[] columnFamily, String[] column,
                              String[] value, int[] num) throws IOException{

        init();

        // 表名对象
        TableName tableName = TableName.valueOf(table_name);
        // 表对象
        Table table = connection.getTable(tableName);
        //ArrayList动态数据,存储多个Put
        ArrayList<Put> list = new ArrayList<Put>();
        int count2 = 0;  // 记录value的下标
        for (int m = 0; m < row.length; m++){
            Put put = new Put(row[m].getBytes("utf8"));  //指定行
            int count1 = 0;    //定位column的位置
            for (int i = 0; i < columnFamily.length; i++){
                for (int j = count1; j < count1 + num[i]; j++){
                    put.addColumn(columnFamily[i].getBytes("utf8"), column[j].getBytes("utf8"),
                            value[count2].getBytes("utf8"));
                    count2++;
                }
                count1 = count1 + num[i];
            }
            put.setDurability(SYNC_WAL);
            list.add(put);
        }
        table.put(list);
        table.close();
        close();

    }


    /**
     * 删除表
     *
     * @param table_name
     * @throws IOException
     */
    public void deleteTable(String table_name) throws IOException{

        init();
        TableName tableName = TableName.valueOf(table_name);
        if (admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        close();

    }


    /**
     * 删除一条记录
     *
     * @param table_name  表名
     * @param row  行主键
     * @throws IOException
     */
    public void delete_row(String table_name, String row) throws IOException{
        //初始化
        init();
        //获得表名对象
        TableName tableName = TableName.valueOf(table_name);
        //获得表对象
        Table table = connection.getTable(tableName);
        //声明删除操作
        Delete del = new Delete(row.getBytes("utf8"));
        //执行删除操作
        table.delete(del);
        //关闭
        close();
    }


    /**
     * 删除多条记录
     *
     * @param table_name 表名
     * @param rows  需要删除的行主键
     * @throws IOException
     */
    public void delMultiRows(String table_name, String[] rows) throws IOException{
        //初始化
        init();
        //表名对象
        TableName tableName = TableName.valueOf(table_name);
        //表对象
        Table table = connection.getTable(tableName);
        //动态数组,储存多个Delete对象(操作)
        ArrayList<Delete> delList = new ArrayList<Delete>();
        //为动态数组赋值,储存多个Delete对象(操作)
        for (String row : rows){
            Delete del = new Delete(row.getBytes("utf8"));
            delList.add(del);
        }
        //执行删除操作了
        table.delete(delList);
        //关闭
        close();
    }


    /**
     * 查询单个row的记录
     *
     * @param table_name  表名
     * @param row  行键
     * @param columnfamily  列族，可以为null
     * @param column  列名，可以为null
     * @return
     * @throws IOException
     */
    public Cell[] getRow(String table_name, String row, String columnfamily, String column) throws IOException{

        //初始化
        init();

        //检查table_name和row是否为空
        if (StringUtils.isEmpty(table_name) || StringUtils.isEmpty(row)){
            return null;
        }

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(table_name));

        //获取对行的get操作
        Get get = new Get(row.getBytes("utf8"));

        // 判断在查询记录时,是否限定列族和列名
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isNotEmpty(column)){
            get.addColumn(columnfamily.getBytes("utf8"), column.getBytes("utf8"));
        }
        if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isEmpty(column)){
            get.addFamily(columnfamily.getBytes("utf8"));
        }

        //获得结果
        Result result = table.get(get);
        //把结果放在Cell数组中
        Cell[] cells = result.rawCells();
        return cells;


    }


    /**
     * 获取表中的部分记录,可以指定列族,列族成员,开始行键,结束行键.
     *
     * @param table_name  表名
     * @param family  列簇
     * @param column  列名
     * @param startRow  开始行主键
     * @param stopRow  结束行主键,结果不包含这行，到它前面一行结束
     * @return
     * @throws Exception
     */
    public ResultScanner scan_part_Table(String table_name, String family, String column, String startRow,
                                         String stopRow) throws IOException {
        //初始化
        init();

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(table_name));
        //scan操作
        Scan scan = new Scan();
        if (StringUtils.isNotEmpty(family) && StringUtils.isNotEmpty(column)) {
            scan.addColumn(family.getBytes("utf8"), column.getBytes("utf8"));
        }
        if (StringUtils.isNotEmpty(family) && StringUtils.isEmpty(column)) {
            scan.addFamily(family.getBytes("utf8"));
        }
        if (StringUtils.isNotEmpty(startRow)) {
            scan.setStartRow(startRow.getBytes("utf8"));
        }
        if (StringUtils.isNotEmpty(stopRow)) {
            scan.setStopRow(stopRow.getBytes("utf8"));
        }
        ResultScanner resultScanner = table.getScanner(scan);
        return resultScanner;
    }


    /**
     * 根据过滤器进行查找
     *
     * @param table_name  表名
     * @param filter  过滤器，就是过滤条件
     * @return
     * @throws IOException
     */
    public ResultScanner scan_filter_Table(String table_name, Filter filter) throws IOException{

        //初始化
        init();
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(table_name));
        //scan操作
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        return resultScanner;
    }


    /**
     * 获取表中的全部记录
     *
     * @param table_name  表名
     * @return
     * @throws IOException
     */
    public ResultScanner scanTableAllData(String table_name) throws IOException{

        //初始化
        init();
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(table_name));
        //scan操作
        Scan scan = new Scan();
        //scan查询
        ResultScanner resultScanner = table.getScanner(scan);
        return resultScanner;
    }


    /**
     * 获得HBase里面所有的表名
     *
     * @return
     * @throws IOException
     */
    public List<String> getAllTables() throws IOException {
        List<String> tables = new ArrayList<String>();
        if (admin != null) {
            HTableDescriptor[] allTable = admin.listTables();
            if (allTable.length > 0) {
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
                }
            }
        }
        return tables;
    }



    /**
     * 获得表的描述
     *
     * @param tableName
     * @return
     */
    public String describeTable(String tableName) {
        try {
            return admin.getTableDescriptor(TableName.valueOf(tableName)).toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 打印多条记录
     * <p>
     * ResultScanner把扫描操作转换为类似的get操作，它将每一行数据封装成一个Result实例，并将所有的Result实例放入一个迭代器中。
     * 用完ResultScanner 后一定要记得关掉：resultScanner.close()
     * rawCells()返回这一行的所有单元格，每个单元格由行主键，列簇，列名，时间戳，值组成
     *
     * @param resultScanner
     */
    public void printallRecoder(ResultScanner resultScanner) {
        for (Iterator<Result> it = resultScanner.iterator(); it.hasNext();) {
            Result result = it.next();
            Cell[] cells = result.rawCells();
            printRecoder(cells);
        }
    }

    /**
     * 打印一条记录
     *
     * @param cells
     */
    public void printRecoder(Cell[] cells) {
        for (Cell cell : cells) {
            // 一个Cell就是一个单元格，通过下面的方法可以获得这个单元格的各个值
            System.out.print("行健: " + new String(CellUtil.cloneRow(cell)));
            System.out.print("列簇: " + new String(CellUtil.cloneFamily(cell)));
            System.out.print(" 列: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
            System.out.println("时间戳: " + cell.getTimestamp());
        }
    }




    public static void main( String[] args ) throws IOException {
        String tableName = "test_test_test";
        App test = new App();
        test.init();
        test.getAllTables();
        test.close();
    }
}
