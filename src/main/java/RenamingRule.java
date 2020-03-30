/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Defines a regular expression to define a substring in which another regular expression defines which part shall be replaced by a replacement string.
 * @author Ulrich Loup <ulrich.loup@dwd.de>
 */
public class RenamingRule {
    private Pattern domainRegex, substituteRegex;
    private String replacement;
    
    /**
     * Constructs a replacement rule.
     * @param domainRegex
     * @param substituteRegex
     * @param replacement 
     */
    public RenamingRule(String domainRegex, String substituteRegex, String replacement) {
        this.domainRegex = Pattern.compile(domainRegex);
        this.substituteRegex = Pattern.compile(substituteRegex);
        this.replacement = replacement;
    }
    
    /**
     * Constructs a replacement rule for removing the substitute characters.
     * @param domainRegex
     * @param substituteRegex
     */
    public RenamingRule(String domainRegex, String substituteRegex) {
        this.domainRegex = Pattern.compile(domainRegex);
        this.substituteRegex = Pattern.compile(substituteRegex);
        this.replacement = "";
    }
    
    /**
     * String representation of the replacement rule.
     * @return String representation of the replacement rule
     */
    public String toString() {
        return this.domainRegex.pattern() + ": " + this.substituteRegex.pattern() + " -> " + (this.replacement.isBlank() ? "''": this.replacement);
    }
    
    /**
     * Replaces every match of {@link #substitutionRegex} with {@link #replacement} inside the first match of {@link #domainRegex} in {@link #s}.
     * @param s The string in which to replace every match of {@link #substituteRegex} with {@link #replacement} inside the first match of {@link #domainRegex}.
     * @return The string constructed by replacing each matching subsequence by the replacement string, substituting captured subsequences as needed
     */
    public String applyIn(CharSequence s) {
        Matcher domainMatcher = this.domainRegex.matcher(s);
        if( domainMatcher.find()) {
            Matcher m = this.substituteRegex.matcher(domainMatcher.group());
            if( m.find() )
                return s.subSequence(0, domainMatcher.start()) + m.replaceAll(this.replacement) + s.subSequence(domainMatcher.end(), s.length());
        }
        return s.toString();
    }
}
