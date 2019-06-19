package main

import (
  "errors"
  "fmt"
  "io/ioutil"
  "crypto/tls"
  "crypto/x509"
  "encoding/pem"
	"dvara"
	corelog "github.com/intercom/gocore/log"
  "strings"
)

type SSLConfig struct {
  tlsConfig *tls.Config
  mongoTLSConfig *tls.Config
}

func parseSubject(pemData []byte) (string, error) {
  var certDERBlock *pem.Block
  for {
    certDERBlock, pemData = pem.Decode(pemData)
    if certDERBlock == nil || certDERBlock.Type == "CERTIFICATE" {
      break
    }
  }

  if certDERBlock != nil {
    if x509Cert, err := x509.ParseCertificate(certDERBlock.Bytes); err == nil {
      subject := x509Cert.Subject
      var subject_names []string

      addComp := func(tag string, c []string) {
        if len(c) == 1 {
          subject_names = append(subject_names, tag+"="+strings.Replace(c[0], ",", "\\,", -1))
        }
      }
      addComp("CN", []string{subject.CommonName})
      addComp("OU", subject.OrganizationalUnit)
      addComp("O",  subject.Organization)
      addComp("L",  subject.Locality)
      addComp("ST", subject.Province)
      addComp("C",  subject.Country)

      subject_name := strings.Join(subject_names, ",")
      return subject_name, nil
    } else {
      return "", err
    }
  }
  return "", errors.New("Unable to find a valid certificate")
}

func NewSSLConfig(sslPEMKeyFile string, mongoSSLPEMKeyFile string, skipVerify bool, cred *dvara.Credential) (*SSLConfig, error) {
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
    if cred.Username == "" && cred.Mechanism == "MONGODB-X509" {
      if username, err := parseSubject(pem); err == nil {
        cred.Username = username
        corelog.LogInfoMessage(fmt.Sprintf("Parsed cert subject as username: %s", cred.Username))
      } else {
        corelog.LogError(fmt.Sprintf("Unable to parse cert: %s", err))
      }
    }
    res.mongoTLSConfig = &tls.Config{
      InsecureSkipVerify: skipVerify,
      Certificates:       []tls.Certificate{cert},
    }
  }
  return res, nil
}
