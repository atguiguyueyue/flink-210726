import java.util.HashMap;

public class test {
    public static void main(String[] args) {
        HashMap<String, String> hashMap = new HashMap<>();

        hashMap.put("a", "b");
        hashMap.put("a", "c");

        System.out.println(hashMap.toString());

    }
}
