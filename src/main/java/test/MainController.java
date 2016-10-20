package test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.context.request.async.DeferredResult;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

@Controller
public class MainController {

    public Client client = new Client();

    public MainController() {
        new Thread(new Server()).start();
        client.run();
    }

    @RequestMapping(value = "/send", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public DeferredResult<String> sendMessage(@RequestParam final String msg) {
        return client.sendOne(msg, msg);
    }

}
