curl \--request POST \\  
     \--url https://app.betterenrich.com/api/v1/find-work-email-low-cost-v3-alt \\  
     \--header 'Authorization: API Key' \\  
     \--header 'accept: application/json' \\  
     \--header 'content-type: application/json' \\  
     \--data '  
{  
  "full\_name": "\<string\>",  
  "company\_domain": "\<string\>",  
  "linkedinURL": "\<string\>"  
}

Rate Limit: 5 reqs per second

P.S. most of the time, it gets back with the result in the same request call but if it returns 201, that means it’s still “in progress” so pls use the following api endpoint to get back the result

curl \--request GET \\  
     \--url 'https://app.betterenrich.com/api/v1/find-work-email-low-cost-v3?id=\<string\>' \\  
     \--header 'Authorization: API Key' \\  
     \--header 'accept: application/json'  
     \--header 'content-type: application/json'  

1: you don’t need to verify the emails again cs we already verify the emails here and you can see email status now.
2: you can send linkedin urls as input with full names and websites that basically helps you get higher coverage.