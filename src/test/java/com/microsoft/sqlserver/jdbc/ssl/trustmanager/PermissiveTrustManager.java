/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.ssl.trustmanager;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.X509TrustManager;


/**
 * This class implements an X509TrustManager that always accepts the X509Certificate chain offered to it.
 */

public final class PermissiveTrustManager implements X509TrustManager {
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {}

    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {}

    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }
}
