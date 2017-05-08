package main

import (
  "io/ioutil"
  "crypto/tls"
)

type SSLConfig struct {
  tlsConfig *tls.Config
  mongoTLSConfig *tls.Config
}

func NewSSLConfig(sslPEMKeyFile string, mongoSSLPEMKeyFile string, skipVerify bool) (*SSLConfig, error) {
  res := &SSLConfig{}
  if sslPEMKeyFile != "" {
    pem, err := ioutil.ReadFile(sslPEMKeyFile)
    if err != nil {
      return nil, err
    }
    cert, err := tls.X509KeyPair(pem, pem)
    if err != nil {
      return nil, err
    }
    res.tlsConfig = &tls.Config{
      Certificates: []tls.Certificate{cert},
    }
  }
  if mongoSSLPEMKeyFile != "" {
    pem, err := ioutil.ReadFile(mongoSSLPEMKeyFile)
    if err != nil {
      return nil, err
    }
    cert, err := tls.X509KeyPair(pem, pem)
    if err != nil {
      return nil, err
    }
    res.mongoTLSConfig = &tls.Config{
      InsecureSkipVerify: skipVerify,
      Certificates:       []tls.Certificate{cert},
    }
  }
  return res, nil
}
