// clang-format off
// Generated from oidc_vars.yml by oidc_vars_gen.py
//#import json

//#def docblock($text, $prefix="// ", $indent="")
//#echo json.dumps($text, indent=4).replace("\n", "\n" + indent + prefix)
//#end def

//#def encaps($text):
//#echo '"' + text.replace('\\', '\\\\').replace('"', '\\"') + '"'
//#end def



function OIDCVars(issuerPrefix) {
  let obj = {};
///////////////////////////////////////////////////////////
// Signed Tokens
///////////////////////////////////////////////////////////

  obj.${global.tokens_varname} = {
//#for $token in $tokens
    // Header: $docblock(token.header, indent="    ")
    // Body: $docblock(token.body, indent="    ")
    ${encaps($token.name)}: ${token.token},
//#end for
  };

///////////////////////////////////////////////////////////
// BSON Payloads
///////////////////////////////////////////////////////////


  obj.${global.payloads_varname} = {
//#for $payload in $payloads
    // Payload: $docblock(payload.data, indent="    ")
    //#if len($payload.referenced_tokens) > 0:
    // Referenced tokens: $repr(payload.referenced_tokens)
    //#end if
    ${encaps($payload.name)}: ${payload.bson},
//#end for
    };

  return obj;
}
