OauthTokenRequest := RECORD
  string client_id {xpath('client_id')} := 'acmepaymentscorp-3rCEQzwEHMT9PPvuXcClpe3v';
  string client_secret {xpath('client_secret')} := '<client_token>';
  string code {xpath('code')} := '<code>';
  string redirect_uri {xpath('redirect_uri')} := 'http://example.com:9900/ui/apps/acmepaymentscorp/resources/console/global/oauthclientredirect.html?dynamic=true';
  string grant_type {xpath('grant_type')} := 'authorization_code';
  string scope {xpath('scope')} := 'Scope1';
END;


OauthTokenResponse := RECORD
  string access_token;
  string id_token;
  string refresh_token;
  string token_type;
  integer2 expires_in;
END;

output(HTTPCALL('https://ocidservice.authexample.io:443/oauth2/token','', OauthTokenRequest, OauthTokenResponse, FORMENCODED('dot')));
