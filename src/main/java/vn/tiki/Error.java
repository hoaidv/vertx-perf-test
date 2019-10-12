package vn.tiki;

import com.dslplatform.json.CompiledJson;

public class Error {

    public int code;
    public String error;

    @CompiledJson
    public Error(int code, String error) {
        this.code = code;
        this.error = error;
    }
}
