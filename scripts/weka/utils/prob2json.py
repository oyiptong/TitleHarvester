#!/usr/bin/env python
import json
import sys
import re
import math

classes = {}
log_priors = []
log_likelihoods = {}

def main():
    with open(sys.argv[1]) as f:
        for line in f:
            line = line.strip()
            tokens = line.split()
            if len(tokens) == 2:
                # it is a class prob line
                log_priors.append(math.log(float(tokens[1])))
                #classes[tokens[0]] = len(priors)-1
                classes[len(log_priors)-1] = tokens[0]
            elif len(log_priors) > 0 and len(tokens) == (len(log_priors) + 1):
                log_token_floats = [math.log(float(x)) for x in tokens[1:]]
                log_likelihoods[tokens[0]] = log_token_floats
    
    data = {
            "classes" : classes,
            "logPriors" : log_priors,
            "logLikelihoods" : log_likelihoods
    }

    with open(sys.argv[2], 'w') as f:
        json.dump(data, f, indent=2)

if __name__ == "__main__":
    main()
