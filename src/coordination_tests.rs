use super::*;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::Command,
};

fn openssl(directory: &Path, args: &[&str]) {
    let executable = if Path::new("/opt/homebrew/opt/openssl@3/bin/openssl").exists() {
        "/opt/homebrew/opt/openssl@3/bin/openssl"
    } else {
        "openssl"
    };
    let result = Command::new(executable)
        .current_dir(directory)
        .args(args)
        .output()
        .expect("test OpenSSL available");
    assert!(
        result.status.success(),
        "synthetic certificate generation failed"
    );
}
fn authority(directory: &Path, stem: &str) -> PathBuf {
    let config = format!("[req]\ndistinguished_name=dn\nx509_extensions=extensions\nprompt=no\n[dn]\nCN=coordination-test-{stem}\n[extensions]\nbasicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\nsubjectKeyIdentifier=hash\n");
    std::fs::write(directory.join(format!("{stem}.cnf")), config).unwrap();
    openssl(
        directory,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-config",
            &format!("{stem}.cnf"),
            "-keyout",
            &format!("{stem}-key.pem"),
            "-out",
            &format!("{stem}.pem"),
        ],
    );
    directory.join(format!("{stem}.pem"))
}
fn identity(directory: &Path) -> native_tls::Identity {
    openssl(
        directory,
        &[
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-keyout",
            "server-key.pem",
            "-out",
            "server.csr",
        ],
    );
    std::fs::write(directory.join("server-extensions.cnf"), "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\nbasicConstraints=critical,CA:FALSE\n").unwrap();
    openssl(
        directory,
        &[
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca-key.pem",
            "-CAcreateserial",
            "-days",
            "1",
            "-extfile",
            "server-extensions.cnf",
            "-out",
            "server.pem",
        ],
    );
    native_tls::Identity::from_pkcs8(
        &std::fs::read(directory.join("server.pem")).unwrap(),
        &std::fs::read(directory.join("server-key.pem")).unwrap(),
    )
    .unwrap()
}
fn handshake(
    identity: native_tls::Identity,
    connector: native_tls::TlsConnector,
    hostname: &str,
) -> bool {
    let acceptor = native_tls::TlsAcceptor::new(identity).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(std::time::Duration::from_secs(3)))
            .unwrap();
        if let Ok(mut tls) = acceptor.accept(stream) {
            let _ = tls.write_all(b"ok");
        }
    });
    let stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(3)))
        .unwrap();
    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(3)))
        .unwrap();
    let accepted = connector.connect(hostname, stream).is_ok_and(|mut tls| {
        let mut response = [0; 2];
        tls.read_exact(&mut response).is_ok() && &response == b"ok"
    });
    server.join().unwrap();
    accepted
}

#[test]
fn custom_ca_verifies_chain_and_hostname_over_real_tls() {
    let temporary = tempfile::tempdir().unwrap();
    let ca = authority(temporary.path(), "ca");
    let other_ca = authority(temporary.path(), "other");
    let server = identity(temporary.path());
    assert!(
        handshake(
            server.clone(),
            tls_connector(Some(&ca)).unwrap(),
            "localhost"
        ),
        "configured CA must authenticate its valid server"
    );
    assert!(!handshake(
        server.clone(),
        tls_connector(Some(&other_ca)).unwrap(),
        "localhost"
    ));
    assert!(!handshake(
        server.clone(),
        tls_connector(Some(&ca)).unwrap(),
        "wrong.invalid"
    ));
    assert!(
        !handshake(server.clone(), tls_connector(None).unwrap(), "localhost"),
        "default OS roots must not silently trust a synthetic CA"
    );
    let bundle = temporary.path().join("bundle.pem");
    let mut bytes = std::fs::read(&ca).unwrap();
    bytes.extend(std::fs::read(&other_ca).unwrap());
    std::fs::write(&bundle, &bytes).unwrap();
    assert!(handshake(
        server,
        tls_connector(Some(&bundle)).unwrap(),
        "localhost"
    ));
    bytes.extend(b"unexpected trailing content");
    std::fs::write(&bundle, bytes).unwrap();
    assert!(tls_connector(Some(&bundle)).is_err());
}

#[test]
fn custom_ca_rejects_missing_empty_and_malformed_input() {
    let temporary = tempfile::tempdir().unwrap();
    let file = temporary.path().join("ca.pem");
    assert!(tls_connector(Some(&file)).is_err());
    for bytes in [
        b"".as_slice(),
        b"not a certificate",
        b"-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----\n",
    ] {
        std::fs::write(&file, bytes).unwrap();
        assert!(tls_connector(Some(&file)).is_err());
    }
}

#[test]
fn ca_configuration_never_silently_accepts_plaintext() {
    let local = "host=127.0.0.1 port=1 sslmode=disable";
    assert!(database_configuration(local, true).is_err());
    assert_eq!(
        database_configuration(local, false).unwrap().get_ssl_mode(),
        SslMode::Disable
    );
    assert!(database_configuration("host=database.invalid sslmode=disable", false).is_err());
    assert_eq!(
        database_configuration("host=database.invalid sslmode=prefer", true)
            .unwrap()
            .get_ssl_mode(),
        SslMode::Require
    );
}
