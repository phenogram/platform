use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, Payload},
};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::error::{AppError, Result};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct Crypto {
    cipher: XChaCha20Poly1305,
    public_id_key: [u8; 32],
    link_signing_key: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct Ciphertext {
    pub nonce: Vec<u8>,
    pub data: Vec<u8>,
}

impl Crypto {
    pub fn new(master_key: &str, public_id_key: &str, link_signing_key: &str) -> Self {
        let master_key = derive_key(master_key, b"phenogram:token-encryption:v1");
        Self {
            cipher: XChaCha20Poly1305::new((&master_key).into()),
            public_id_key: derive_key(public_id_key, b"phenogram:public-id:v1"),
            link_signing_key: derive_key(link_signing_key, b"phenogram:file-link:v1"),
        }
    }

    pub fn encrypt(&self, plaintext: &[u8], context: &[u8]) -> Result<Ciphertext> {
        let mut nonce = [0_u8; 24];
        getrandom::fill(&mut nonce).map_err(|e| AppError::Crypto(e.to_string()))?;
        let data = self
            .cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: context,
                },
            )
            .map_err(|_| AppError::Crypto("encryption failed".into()))?;
        Ok(Ciphertext {
            nonce: nonce.to_vec(),
            data,
        })
    }

    pub fn decrypt(&self, encrypted: &Ciphertext, context: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        if encrypted.nonce.len() != 24 {
            return Err(AppError::Crypto("invalid nonce".into()));
        }
        let bytes = self
            .cipher
            .decrypt(
                XNonce::from_slice(&encrypted.nonce),
                Payload {
                    msg: &encrypted.data,
                    aad: context,
                },
            )
            .map_err(|_| AppError::Crypto("decryption failed".into()))?;
        Ok(Zeroizing::new(bytes))
    }

    pub fn bot_public_id(&self, token: &str, telegram_test_dc: bool) -> String {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.public_id_key)
            .expect("HMAC accepts a 32-byte key");
        mac.update(telegram_credential_domain(telegram_test_dc));
        mac.update(token.as_bytes());
        let digest = mac.finalize().into_bytes();
        format!("phg_{}", URL_SAFE_NO_PAD.encode(&digest[..18]))
    }

    pub fn token_fingerprint(token: &str, telegram_test_dc: bool) -> String {
        let mut digest = Sha256::new();
        digest.update(telegram_credential_domain(telegram_test_dc));
        digest.update(token.as_bytes());
        let digest = digest.finalize();
        format!(
            "{}…{}",
            &token[..token.len().min(5)],
            URL_SAFE_NO_PAD.encode(&digest[..5])
        )
    }

    pub fn digest_secret(secret: &[u8]) -> Vec<u8> {
        Sha256::digest(secret).to_vec()
    }

    pub fn verify_secret(secret: &[u8], digest: &[u8]) -> bool {
        let candidate = Self::digest_secret(secret);
        candidate.as_slice().ct_eq(digest).into()
    }

    pub fn random_token(bytes: usize) -> Result<String> {
        let mut token = vec![0_u8; bytes];
        getrandom::fill(&mut token).map_err(|e| AppError::Crypto(e.to_string()))?;
        Ok(URL_SAFE_NO_PAD.encode(token))
    }

    pub fn sign_file_link(&self, public_id: &str, file_path: &str, expires: i64) -> String {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.link_signing_key)
            .expect("HMAC accepts a 32-byte key");
        mac.update(b"file:v1\n");
        mac.update(public_id.as_bytes());
        mac.update(b"\n");
        mac.update(file_path.as_bytes());
        mac.update(b"\n");
        mac.update(expires.to_string().as_bytes());
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    }

    pub fn csrf_token(&self, session_token: &str) -> String {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.link_signing_key)
            .expect("HMAC accepts a 32-byte key");
        mac.update(b"csrf:v1:");
        mac.update(session_token.as_bytes());
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    }

    pub fn verify_file_link(
        &self,
        public_id: &str,
        file_path: &str,
        expires: i64,
        signature: &str,
        now: i64,
    ) -> bool {
        if expires < now || expires > now.saturating_add(86_400 * 7) {
            return false;
        }
        let expected = self.sign_file_link(public_id, file_path, expires);
        expected.as_bytes().ct_eq(signature.as_bytes()).into()
    }
}

fn telegram_credential_domain(telegram_test_dc: bool) -> &'static [u8] {
    if telegram_test_dc {
        b"telegram-dc:v1:test\0"
    } else {
        b"telegram-dc:v1:prod\0"
    }
}

fn derive_key(value: &str, domain: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(b"\0");
    hash.update(value.as_bytes());
    hash.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crypto() -> Crypto {
        Crypto::new(&"a".repeat(32), &"b".repeat(32), &"c".repeat(32))
    }

    #[test]
    fn encrypted_values_require_the_same_context() {
        let crypto = crypto();
        let encrypted = crypto.encrypt(b"secret", b"bot:1").unwrap();
        assert_eq!(&*crypto.decrypt(&encrypted, b"bot:1").unwrap(), b"secret");
        assert!(crypto.decrypt(&encrypted, b"bot:2").is_err());
    }

    #[test]
    fn public_ids_are_stable_but_do_not_contain_the_token() {
        let crypto = crypto();
        let id = crypto.bot_public_id("123:secret", false);
        assert_eq!(id, crypto.bot_public_id("123:secret", false));
        assert!(id.starts_with("phg_"));
        assert!(!id.contains("secret"));
        assert_ne!(id, crypto.bot_public_id("123:secret", true));
    }

    #[test]
    fn credential_identifiers_are_domain_separated_by_telegram_environment() {
        let crypto = crypto();
        assert_eq!(
            crypto.bot_public_id("123:secret", false),
            "phg_8nXOV-QrC3mmm517ijpZlMjV"
        );
        assert_eq!(
            crypto.bot_public_id("123:secret", true),
            "phg_kD1sFex4hdK1HJcOC4JG4E5k"
        );
        assert_ne!(
            Crypto::token_fingerprint("123:secret", false),
            Crypto::token_fingerprint("123:secret", true)
        );
    }

    #[test]
    fn file_links_are_scoped_and_expire() {
        let crypto = crypto();
        let signature = crypto.sign_file_link("phg_1", "photos/a.jpg", 200);
        assert!(crypto.verify_file_link("phg_1", "photos/a.jpg", 200, &signature, 100));
        assert!(!crypto.verify_file_link("phg_1", "photos/b.jpg", 200, &signature, 100));
        assert!(!crypto.verify_file_link("phg_1", "photos/a.jpg", 200, &signature, 201));
    }
}
