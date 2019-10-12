package vn.tiki;

import com.dslplatform.json.CompiledJson;

public class Voucher {

    public int id;

    public String code;

    public int quantity;

    @CompiledJson
    public Voucher(int id, String code, int quantity) {
        this.id = id;
        this.code = code;
        this.quantity = quantity;
    }
}
