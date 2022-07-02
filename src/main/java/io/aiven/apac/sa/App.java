package io.aiven.apac.sa;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Hello world!
 *
 */
public class App {

    public static void main(String[] args) {
    
        App a = new App();
        try {
            a.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

    private void run() throws Exception {
        Dotenv dotenv = Dotenv.load();
        System.out.println(String.format("CA CERT PATh : %s",dotenv.get("caCertPath")));
        SSLOptions sslOptions = loadCaCert(dotenv.get("caCertPath"));

        Cluster c = null;
        try {
            c = Cluster.builder()
                .addContactPoint(dotenv.get("host"))
                .withPort(Integer.parseInt(dotenv.get("port")))
                .withSSL(sslOptions)
                .withAuthProvider(new PlainTextAuthProvider(dotenv.get("username"), dotenv.get("password")))
                .build();

            Session s = c.connect();
            s.execute(
                "CREATE KEYSPACE IF NOT EXISTS troysTestSpace WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'aiven': 3}"
            );
            s.execute("USE troysTestSpace");
            s.execute("CREATE TABLE IF NOT EXISTS troysTable(id int PRIMARY KEY, message text)");
            s.execute("INSERT INTO troysTable(id, message) VALUES (?,?)", 123, "Hello from java");
            ResultSet rs = s.execute("SELECT * from troysTable");
            for (Row r : rs) {
                System.out.println(String.format("Row: id = %d, message: %s", r.getInt("id"), r.getString("message")));
            }
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    private static SSLOptions loadCaCert(String caCertPath) throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        FileInputStream fis = null;
        X509Certificate caCert;
        try {
            fis = new FileInputStream(caCertPath);
            caCert = (X509Certificate) cf.generateCertificate(fis);
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
        
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null);
        ks.setCertificateEntry("caCert", caCert);
        tmf.init(ks);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmf.getTrustManagers(), null);
        return RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).build();
    }
}
