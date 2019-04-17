import sqlalchemy
import socket
import requests
import random
import string
import ipaddress
import datetime
import time
import uuid
import psycopg2
from colorama import Fore, Style
from .  import tests_info_db
import os
import threading

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes

def get_unused_port(start, db_retries=30):
    assert db_retries > 0
    port = start
    if is_port_open(port):
        return get_unused_port(port + 1, db_retries)
    try:
        tests_info_db.reserve_port(port)
    except Exception as e:
        print('Error (tries remaining: {}) :'.format(db_retries),e)
        time.sleep(random.uniform(0.1,1.5))
        return get_unused_port(port+1, db_retries -1)
    return port

def stop_docker_and_collect_logs( cntnr, log_file):
    print(Fore.YELLOW, "Stopping HGE docker container ", cntnr.name, Style.RESET_ALL)
    cntnr.stop()

    print(Fore.YELLOW, "Collecting logs of HGE docker container ", cntnr.name, Style.RESET_ALL)
    with open(log_file,'wb') as f:
        f.write( cntnr.logs(stdout=True, stderr=True) )

    print(Fore.YELLOW, "Removing HGE docker container ", cntnr.name, Style.RESET_ALL)
    cntnr.remove()

def pg_create_database(pg_url, db, exists_ok = False):
    run_db_query = True
    if exists_ok:
        r = run_sql(pg_url, "select datname from pg_database where datname='" + db + "'")
        run_db_query = r.rowcount == 0
    if run_db_query:
        pg_run_sql_without_transaction(pg_url, "create database " + db)

def pg_run_sql_without_transaction(pg_url, sql):
    with psycopg2.connect(pg_url) as conn:
        conn.autocommit = True
        with conn.cursor() as curs:
            res = curs.execute(sql)
    return res

def run_sql(pg_url, sql):
    #print("Postgres url: ", pg_url,", Query: ",sql)
    with sqlalchemy.create_engine(pg_url).connect() as conn:
        res = conn.execute(sql)
    return res

def is_graphql_server_running(hge_url):
    r = requests.get(hge_url+'/v1/version')
    return r.status_code == 200

def gen_random_password(length=6):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))

def is_port_open(port):
    if tests_info_db.is_port_reserved(port):
        return True

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        res = sock.connect_ex(('127.0.0.1', port))
        return res == 0

def gen_rsa_key():
    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )

def get_private_pem(private_key):
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()
    ).decode('ascii')

def get_public_pem(private_key):
    public_key = private_key.public_key()
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode('ascii')

def get_public_crt(crt):
    return crt.public_bytes(
        encoding=serialization.Encoding.PEM,
    ).decode('ascii')

def gen_ca_cert(ca_private_key, days_to_expiry=5):
    ca_public_key = ca_private_key.public_key()

    builder = x509.CertificateBuilder()
    builder = builder.subject_name(x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, u'webhook CA'),
    ]))
    builder = builder.issuer_name(x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, u'webhook CA'),
    ]))
    one_day = datetime.timedelta(1, 0, 0)
    builder = builder.not_valid_before(datetime.datetime.today() - one_day)
    builder = builder.not_valid_after(datetime.datetime.today() + days_to_expiry*one_day)
    builder = builder.serial_number(int(uuid.uuid4()))
    builder = builder.public_key(ca_public_key)
    builder = builder.add_extension(
        x509.SubjectKeyIdentifier.from_public_key(ca_public_key),
        critical=False,
    ).add_extension(
        extension=x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_public_key),
        critical=False
    ).add_extension(
        x509.BasicConstraints(ca=True, path_length=None), critical=True,
    )

    return builder.sign(
        private_key=ca_private_key, algorithm=hashes.SHA256(),
        backend=default_backend()
    )


def gen_ca_keys_and_cert(days_to_expiry=5):
    pkey = gen_rsa_key()
    cert = gen_ca_cert(pkey, days_to_expiry)
    return (pkey, cert)

def gen_csr(private_key):
    return x509.CertificateSigningRequestBuilder().subject_name(x509.Name([
        # Provide various details about who we are.
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"IN"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"Karnataka"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, u"Bangalore"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Foo"),
        x509.NameAttribute(NameOID.COMMON_NAME, u"webhook-ca"),
    ])).add_extension(
        x509.SubjectAlternativeName([
            # Describe what sites we want this certificate for.
            x509.DNSName(u"localhost"),
            x509.IPAddress(ipaddress.IPv4Address('127.0.0.1'))
        ]),
        critical=False,
    # Sign the CSR with our private key.
    ).sign(private_key, hashes.SHA256(), default_backend())

def sign_csr(csr, ca_key, ca_cert,days_to_expiry=5):
    return x509.CertificateBuilder().subject_name(
        csr.subject
    ).issuer_name(
        ca_cert.subject
    ).public_key(
        csr.public_key()
    ).serial_number(
        uuid.uuid4().int  # pylint: disable=no-member
    ).not_valid_before(
        datetime.datetime.utcnow()
    ).not_valid_after(
        datetime.datetime.utcnow() + datetime.timedelta(days=days_to_expiry)
    ).add_extension(
        extension=x509.KeyUsage(
            digital_signature=True, key_encipherment=True, content_commitment=True,
            data_encipherment=False, key_agreement=False, encipher_only=False, decipher_only=False, key_cert_sign=False, crl_sign=False
        ),
        critical=True
    ).add_extension(
        extension=x509.BasicConstraints(ca=False, path_length=None),
        critical=True
    ).add_extension(
        extension=x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
        critical=False
    ).add_extension(
        x509.SubjectAlternativeName([
            # Describe what sites we want this certificate for.
            x509.DNSName(u"localhost"),
            x509.IPAddress(ipaddress.IPv4Address('127.0.0.1'))
        ]),
        critical=False,
    # Sign the CSR with our private key.
    ).sign(
        private_key=ca_key,
        algorithm=hashes.SHA256(),
        backend=default_backend()
    )

def gen_ca_signed_keys_and_cert(ca_key, ca_cert, days_to_expiry=5):
    pkey = gen_rsa_key()
    csr = gen_csr(pkey)
    cert = sign_csr(csr, ca_key, ca_cert, days_to_expiry)
    return (pkey, cert)

def unique_list(x):
    return list(set(x))

def run_concurrently(threads):
    for t in threads:
        t.start()

    for t in threads:
        t.join()
