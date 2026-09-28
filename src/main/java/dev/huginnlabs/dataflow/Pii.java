package dev.huginnlabs.dataflow;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

/**
 * Client-side PII classification: payload field names are matched against
 * privacy keywords and reduced to category labels ("email,phone"). Only
 * labels travel in metadata — values stay in the E2E-encrypted payload.
 *
 * <p>Multiword keywords ("first_name") match by substring, single tokens
 * ("card", "ip") by exact word match, so "description" never lights up the
 * "ip" category.
 */
public final class Pii {

    private record Category(String name, String[] keywords) {}

    private static final Category[] CATEGORIES = {
        new Category("password", new String[]{"password", "passwd", "pwd"}),
        new Category("secret", new String[]{"token", "secret", "apikey", "api_key", "credential", "session", "jwt", "auth"}),
        new Category("payment", new String[]{"card", "pan", "cvv", "cvc", "iban", "expiry"}),
        new Category("email", new String[]{"email", "e_mail", "mail"}),
        new Category("phone", new String[]{"phone", "mobile", "tel", "msisdn"}),
        new Category("government_id", new String[]{"ssn", "passport", "tax_id", "national_id"}),
        new Category("birth", new String[]{"birth", "dob", "age"}),
        new Category("name", new String[]{"first_name", "last_name", "full_name", "surname", "customer_name", "display_name"}),
        new Category("address", new String[]{"street", "zip", "postal", "street_address", "postal_address",
                "home_address", "billing_address", "shipping_address", "mailing_address"}),
        new Category("geo", new String[]{"city", "country", "region", "location", "lat", "lon", "lng"}),
        new Category("ip", new String[]{"ip", "ip_address", "client_ip", "remote_addr"}),
        new Category("device", new String[]{"device", "user_agent", "imei", "fingerprint"}),
    };

    private Pii() {}

    /** Maps field names to deduplicated, comma-joined privacy categories. */
    public static String classify(Collection<String> fields) {
        Set<String> seen = new TreeSet<>();
        for (String field : fields) {
            String norm = field.toLowerCase().replaceAll("[^a-z0-9_]+", "_");
            Set<String> tokens = new TreeSet<>(java.util.List.of(norm.split("_")));
            for (Category cat : CATEGORIES) {
                if (seen.contains(cat.name())) continue;
                for (String kw : cat.keywords()) {
                    boolean hit = kw.contains("_")
                            ? norm.contains(kw)
                            : tokens.contains(kw);
                    if (hit) {
                        seen.add(cat.name());
                        break;
                    }
                }
            }
        }
        return String.join(",", seen);
    }
}
