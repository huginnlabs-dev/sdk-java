package dev.huginnlabs.dataflow;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import dev.huginnlabs.dataflow.gen.DataflowProto;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Client-side payload protection, wire-compatible with the Go/Python/C++
 * SDKs: AES-256-GCM over the JSON payload, key derived from the user's
 * secret via PBKDF2-SHA256 (10 000 iterations, 16-byte random process
 * salt). The secret never leaves the host; the server stores ciphertext
 * plus the salt needed to re-derive the key in the user's own dashboard.
 */
final class Crypto {

    static final int KEY_LEN = 32;
    static final int IV_LEN = 12;
    static final int SALT_LEN = 16;
    static final int ITERATIONS = 10_000;
    private static final int GCM_TAG_BITS = 128;

    private static final SecureRandom RNG = new SecureRandom();
    private static volatile SecretKey key = null;
    private static volatile String saltHex = "";

    private Crypto() {}

    /** Derives the process key once; returns false without a configured secret. */
    static synchronized boolean init(String secret) {
        if (key != null) return true; // one key per process, same salt
        if (secret == null || secret.isEmpty()) return false;
        byte[] salt = new byte[SALT_LEN];
        RNG.nextBytes(salt);
        key = derive(secret, salt);
        StringBuilder sb = new StringBuilder(SALT_LEN * 2);
        for (byte b : salt) sb.append(String.format("%02x", b));
        saltHex = sb.toString();
        return true;
    }

    static boolean enabled() { return key != null; }

    static String saltHex() { return saltHex; }

    private static SecretKey derive(String secret, byte[] salt) {
        try {
            PBEKeySpec spec = new PBEKeySpec(secret.toCharArray(), salt, ITERATIONS, KEY_LEN * 8);
            SecretKeyFactory f = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            return new SecretKeySpec(f.generateSecret(spec).getEncoded(), "AES");
        } catch (Exception e) {
            throw new IllegalStateException("dataflow: key derivation failed", e);
        }
    }

    /**
     * Serializes the payload snapshot to JSON and encrypts it when a key is
     * configured; falls back to plaintext on any crypto failure (visibility
     * beats silence) — mirroring the other SDKs.
     */
    static DataflowProto.PayloadData seal(Map<String, Object> data) {
        byte[] plain = Json.write(data).getBytes(StandardCharsets.UTF_8);
        SecretKey k = key;
        if (k == null) {
            return DataflowProto.PayloadData.newBuilder()
                    .setEncrypted(false)
                    .setData(com.google.protobuf.ByteString.copyFrom(plain))
                    .build();
        }
        try {
            byte[] iv = new byte[IV_LEN];
            RNG.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, k, new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] ct = cipher.doFinal(plain); // ciphertext || GCM tag
            return DataflowProto.PayloadData.newBuilder()
                    .setEncrypted(true)
                    .setData(com.google.protobuf.ByteString.copyFrom(ct))
                    .setIv(com.google.protobuf.ByteString.copyFrom(iv))
                    .setKeySalt(saltHex)
                    .build();
        } catch (Exception e) {
            Dataflow.settings().logger.accept("dataflow: payload encryption failed: " + e);
            return DataflowProto.PayloadData.newBuilder()
                    .setEncrypted(false)
                    .setData(com.google.protobuf.ByteString.copyFrom(plain))
                    .build();
        }
    }
}
