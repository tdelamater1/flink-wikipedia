package xyz.delamater;

public class EditEvent implements java.io.Serializable {
    public String id;
    public String domain;
    public String namespace;
    public String title;
    public String timestamp;
    public String user_name;
    public String user_type;
    public long old_length;
    public long new_length;

    public EditEvent() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public String getUser_type() {
        return user_type;
    }

    public void setUser_type(String user_type) {
        this.user_type = user_type;
    }

    public long getOld_length() {
        return old_length;
    }

    public void setOld_length(long old_length) {
        this.old_length = old_length;
    }

    public long getNew_length() {
        return new_length;
    }

    public void setNew_length(long new_length) {
        this.new_length = new_length;
    }
}