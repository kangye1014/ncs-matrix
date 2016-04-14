package com.cubead.performance.martix;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

/**
 * 维度 <br>
 * 多个字段组成
 * 
 * @author kangye
 */
public class Dimension {

    private List<Dimen> dimens;

    public Dimension(Set<String> fields) {

        dimens = new ArrayList<>();
        if (null == fields)
            return;
        for (String dimen : fields) {
            dimens.add(new Dimen(dimen));
        }
    }

    public String parseAsKey() {

        if (CollectionUtils.isEmpty(dimens))
            return null;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < dimens.size(); i++) {
            sb.append(dimens.get(i).value);
            sb.append("-");
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        String key = parseAsKey();
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Dimension other = (Dimension) obj;
        if (dimens == null) {
            if (other.dimens != null)
                return false;
        } else if (!dimens.equals(other.dimens))
            return false;
        return true;
    }

    /**
     * 字段域
     */
    public static class Dimen {

        private String field;
        private Object value;

        public void setValue(Object value) {
            this.value = value;
        }

        public String getField() {
            return field;
        }

        public Dimen(String field) {
            super();
            this.field = field;
        }

        @Override
        public String toString() {
            return "Dimen [field=" + field + ", value=" + value + "]";
        }
    }

}
