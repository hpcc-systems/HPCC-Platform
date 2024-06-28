import{_ as a,c as s,o as e,V as n}from"./chunks/framework.gBlNPWt_.js";const _=JSON.parse('{"title":"ECL standard library style guide","description":"","frontmatter":{},"headers":[],"relativePath":"ecllibrary/StyleGuide.md","filePath":"ecllibrary/StyleGuide.md","lastUpdated":1719574263000}'),t={name:"ecllibrary/StyleGuide.md"},l=n(`<h1 id="ecl-standard-library-style-guide" tabindex="-1">ECL standard library style guide <a class="header-anchor" href="#ecl-standard-library-style-guide" aria-label="Permalink to &quot;ECL standard library style guide&quot;">​</a></h1><p>The ECL code in the standard library should follow the following style guidelines:</p><ul><li>All ECL keywords in upper case</li><li>ECL reserved types in upper case</li><li>Public attributes in camel case with leading upper case</li><li>Private attributes in lower case with underscore as a separator</li><li>Field names in lower case with underscore as a separator</li><li>Standard indent is 2 spaces (no tabs)</li><li>Maximum line length of 120 characters</li><li>Compound statements have contents indented, and END is aligned with the opening statement</li><li>Field names are not indented to make them line up within a record structure</li><li>Parameters are indented as necessary</li><li>Use javadoc style comments on all functions/attributes (see <a href="http://java.sun.com/j2se/javadoc/writingdoccomments/" target="_blank" rel="noreferrer">Writing Javadoc Comments</a>)</li></ul><p>For example:</p><div class="language-ecl vp-adaptive-theme"><button title="Copy Code" class="copy"></button><span class="lang">ecl</span><pre class="shiki shiki-themes github-light github-dark vp-code"><code><span class="line"><span>my_record := RECORD</span></span>
<span class="line"><span>    INTEGER4 id;</span></span>
<span class="line"><span>    STRING firstname{MAXLENGTH(40)};</span></span>
<span class="line"><span>    STRING lastname{MAXLENGTH(50)};</span></span>
<span class="line"><span>END;</span></span>
<span class="line"><span></span></span>
<span class="line"><span>/**</span></span>
<span class="line"><span>  * Returns a dataset of people to treat with caution matching a particular lastname.  The</span></span>
<span class="line"><span>  * names are maintained in a global database of undesirables.</span></span>
<span class="line"><span>  *</span></span>
<span class="line"><span>  * @param  search_lastname    A lastname used as a filter</span></span>
<span class="line"><span>  * @return                    The list of people</span></span>
<span class="line"><span>  * @see                       NoFlyList</span></span>
<span class="line"><span>  * @see                       MorePeopleToAvoid</span></span>
<span class="line"><span>  */</span></span>
<span class="line"><span></span></span>
<span class="line"><span>EXPORT DodgyCharacters(STRING search_lastname) := FUNCTION</span></span>
<span class="line"><span>    raw_ds := DATASET(my_record, &#39;undesirables&#39;, THOR);</span></span>
<span class="line"><span>    RETURN raw_ds(last_name = search_lastname);</span></span>
<span class="line"><span>END;</span></span></code></pre></div><p>Some additional rules for attributes in the library:</p><ul><li>Services should be SHARED and EXPORTed via intermediate attributes</li><li>All attributes must have at least one matching test. If you&#39;re not on the test list you&#39;re not coming in.</li></ul>`,7),i=[l];function p(r,o,c,d,u,m){return e(),s("div",null,i)}const y=a(t,[["render",p]]);export{_ as __pageData,y as default};
