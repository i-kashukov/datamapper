public class Solution {

    public static void main(String[] args) {
        CachingDataMapper<String> mapper = new MysqlCachingStringMapper(
                "localhost","testdb",
                "mytable","root","12345");
        mapper.persist("Test string");
        System.out.println(mapper.size());
        mapper.persist("Test string");
        long id = mapper.persist("String 2");
        System.out.println(mapper.size());
        mapper.delete("Test string");
        System.out.println(mapper.size());
        System.out.println(mapper.getById(id));

    }
}
