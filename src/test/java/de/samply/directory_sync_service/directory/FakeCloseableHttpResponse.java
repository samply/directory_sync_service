package de.samply.directory_sync_service.directory;

import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpEntity;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Fake implementation of CloseableHttpResponse, this class is used solely for testing.
 */
class FakeCloseableHttpResponse implements CloseableHttpResponse {
    private final StatusLine statusLine;
    private final HttpEntity entity;
    FakeCloseableHttpResponse(int code, String reason, String body) {
        this.statusLine = new BasicStatusLine(new ProtocolVersion("HTTP",1,1), code, reason);
        this.entity = body == null ? null : new StringEntity(body, StandardCharsets.UTF_8);
    }
    @Override public StatusLine getStatusLine() { return statusLine; }
    @Override public HttpEntity getEntity() { return entity; }
    @Override public void close() {}
    // return empty/defaults for the rest:
    @Override public void setEntity(HttpEntity e) {}
    @Override public Locale getLocale() { return Locale.getDefault(); }
    @Override public void setLocale(Locale loc) {}
    @Override public ProtocolVersion getProtocolVersion() { return statusLine.getProtocolVersion(); }
    @Override public boolean containsHeader(String name){ return false; }
    @Override public Header[] getHeaders(String name){ return new Header[0]; }
    @Override public Header getFirstHeader(String name){ return null; }
    @Override public Header getLastHeader(String name){ return null; }
    @Override public Header[] getAllHeaders(){ return new Header[0]; }
    @Override public void addHeader(Header header){}
    @Override public void addHeader(String name, String value){}
    @Override public void setHeader(Header header){}
    @Override public void setHeader(String name, String value){}
    @Override public void setHeaders(Header[] headers){}
    @Override public void removeHeader(Header header){}
    @Override public void removeHeaders(String name){}
    @Override public HeaderIterator headerIterator(){ return null; }
    @Override public HeaderIterator headerIterator(String name){ return null; }
    @Override public void setStatusLine(StatusLine statusline){}
    @Override public void setStatusLine(ProtocolVersion ver, int code){}
    @Override public void setStatusLine(ProtocolVersion ver, int code, String reason){}
    @Override public void setStatusCode(int code) throws IllegalStateException {}
    @Override public void setReasonPhrase(String reason) throws IllegalStateException {}

    @Override
    @Deprecated
    public org.apache.http.params.HttpParams getParams() {
        return null;
    }

    @Override
    @Deprecated
    public void setParams(org.apache.http.params.HttpParams params) {
        // no-op
    }
}
