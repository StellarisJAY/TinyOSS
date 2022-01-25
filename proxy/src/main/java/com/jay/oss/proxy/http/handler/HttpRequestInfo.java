package com.jay.oss.proxy.http.handler;

import io.netty.handler.codec.http.HttpMethod;

import java.util.Objects;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2022/01/25 14:42
 */
public class HttpRequestInfo {
    private String path;
    private HttpMethod method;

    public HttpRequestInfo(String path, HttpMethod method) {
        this.path = path;
        this.method = method;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpRequestInfo that = (HttpRequestInfo) o;
        return Objects.equals(path, that.path) && Objects.equals(method, that.method);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, method);
    }
}
