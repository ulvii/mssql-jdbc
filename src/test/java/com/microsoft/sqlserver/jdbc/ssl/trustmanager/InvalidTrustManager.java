/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.ssl.trustmanager;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;


/**
 * This class does not implement X509TrustManager and the connection must fail when it is specified by the
 * trustManagerClass property
 * 
 */
public final class InvalidTrustManager {
    public InvalidTrustManager() {}

    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {

    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {}

    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }
}
