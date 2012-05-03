package com.yahoo.ycsb.riak;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.util.CharsetUtils;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import org.apache.http.impl.cookie.DateParseException;
import org.apache.http.impl.cookie.DateUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.json.JSONException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.basho.riak.client.builders.RiakObjectBuilder.newBuilder;
import static com.basho.riak.client.http.util.Constants.HDR_USERMETA_REQ_PREFIX;
import static com.yahoo.ycsb.StringByteIterator.getStringMap;
import static java.lang.Character.toLowerCase;

@SuppressWarnings({"unchecked"})
public class RiakConversionUtils {

    private static final Pattern CHARSET_PATTERN = Pattern.compile(
            "\\bcharset *= *\"?([^ ;\"]+)\"?", Pattern.CASE_INSENSITIVE);

    private static final String BUCKET_PROPERTY = "bucket";
    private static final String KEY_PROPERTY = "key";
    private static final String VCLOCK_PROPERTY = "vclock";
    private static final String VALUES_PROPERTY = "values";
    private static final String METADATA_PROPERTY = "metadata";
    private static final String DATA_PROPERTY = "data";
    private static final String CONTENT_TYPE_PROPERTY = "content-type";
    private static final String LINKS_PROPERTY = "Links";

    private static final String X_RIAK_VTAG = "X-Riak-VTag";
    private static final String X_RIAK_LAST_MODIFIED = "X-Riak-Last-Modified";
    private static final String X_RIAK_META = "X-Riak-Meta";

    private final Charset charset;
    private final ObjectMapper mapper = new ObjectMapper();

    public RiakConversionUtils(Charset charset) {
        this.charset = charset;
    }

    public byte[] toRiakValue(Map<String, ByteIterator> values)
            throws JSONException {
        ObjectNode value = mapper.createObjectNode();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            value.put(entry.getKey(), entry.getValue().toString());
        }
        return value.toString().getBytes(charset);
    }

    private Charset getCharset(String contentType) {
        if (contentType == null) {
            return charset;
        }
        Matcher matcher = CHARSET_PATTERN.matcher(contentType);
        if (matcher.find()) {
            String encoding = matcher.group(1);
            try {
                return Charset.forName(encoding.toUpperCase());
            } catch (Exception e) {
                // ignore //
            }
        }
        return charset;
    }

    public IRiakObject toRiakObject(JsonNode node) {
        RiakObjectBuilder builder = newBuilder(node.get(BUCKET_PROPERTY).asText(), node.get(KEY_PROPERTY).asText());
        String vclock = node.get(VCLOCK_PROPERTY).asText();
        builder.withVClock(CharsetUtils.utf8StringToBytes(vclock));
        ArrayNode values = (ArrayNode) node.get(VALUES_PROPERTY);
        if (values.size() != 0) {
            ObjectNode value = (ObjectNode) values.get(0);
            ObjectNode meta = (ObjectNode) value.get(METADATA_PROPERTY);

            builder.withValue(value.get(DATA_PROPERTY).asText());
            builder.withContentType(meta.get(CONTENT_TYPE_PROPERTY).asText());
            builder.withVtag(meta.get(X_RIAK_VTAG).asText());

            try {
                Date lastModDate = DateUtils.parseDate(meta.get(X_RIAK_LAST_MODIFIED).asText());
                builder.withLastModified(lastModDate.getTime());
            } catch (DateParseException e) {
                // NO-OP
            }
            JsonNode links = meta.get(LINKS_PROPERTY);
            if (links != null) {
                for (JsonNode o : links) {
                    ArrayNode link = (ArrayNode) o;
                    builder.addLink(link.get(0).asText(), link.get(1).asText(), link.get(2).asText());
                }
            }
            ObjectNode userMeta = (ObjectNode) meta.get(X_RIAK_META);
            Iterator<Map.Entry<String, JsonNode>> iterator = userMeta.getFields();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = iterator.next();

                builder.addUsermeta(checkMetaKey(entry.getKey()), entry.getValue().asText());
            }
        }
        return builder.build();
    }

    public Map<String, String> toRiakMeta(Map<String, ByteIterator> values) {
        return getStringMap(values);
    }

    public void fromRiakValue(IRiakObject object, Set<String> fields, Map<String, ByteIterator> result)
            throws IOException {
        byte[] value = object.getValue();
        Charset charset = getCharset(object.getContentType());
        JsonNode node = mapper.readTree(CharsetUtils.asString(value, charset));
        if (fields == null) {
            Iterator<Map.Entry<String, JsonNode>> iterator = node.getFields();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                result.put(entry.getKey(), new StringByteIterator(entry.getValue().toString()));
            }
        } else {
            for (String field : fields) {
                result.put(field, new StringByteIterator(node.get(field).toString()));
            }
        }
    }

    public void fromRiakMeta(IRiakObject object, Set<String> fields, Map<String, ByteIterator> result) {
        Map<String, String> meta = object.getMeta();
        if (fields == null) {
            for (Map.Entry<String, String> entry : meta.entrySet()) {
                result.put(checkMetaKey(entry.getKey()), new StringByteIterator(entry.getValue()));
            }
        } else {
            for (String field : fields) {
                result.put(field, new StringByteIterator(object.getUsermeta(field)));
            }
        }
    }

    private static String checkMetaKey(String metaKey) {
        if (metaKey.startsWith(HDR_USERMETA_REQ_PREFIX)) {
            String key = metaKey.substring(HDR_USERMETA_REQ_PREFIX.length());
            return new StringBuilder().append(toLowerCase(key.charAt(0))).append(key, 1, key.length()).toString();
        } else {
            return metaKey;
        }
    }
}