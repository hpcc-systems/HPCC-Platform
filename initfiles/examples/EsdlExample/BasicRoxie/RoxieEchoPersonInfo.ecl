import $.esdl_example;
request := dataset([], esdl_example.t_RoxieEchoPersonInfoRequest) : stored('RoxieEchoPersonInfoRequest', few);
output(request, named('RoxieEchoPersonInfoResponse'));
