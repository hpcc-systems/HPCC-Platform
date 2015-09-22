package EsdlExample;

public class EsdlExampleService extends EsdlExampleServiceBase
{
    int counter=0;
    public JavaEchoPersonInfoResponse JavaEchoPersonInfo(EsdlContext context, JavaEchoPersonInfoRequest request)
    {
        System.out.println("Method EchoPersonInfo of Service EsdlExample called!");
        JavaEchoPersonInfoResponse  response = new JavaEchoPersonInfoResponse();
        response.count = new Integer(++counter);
        response.Name = request.Name;
        response.Addresses = request.Addresses;
        return response;
    }
}

