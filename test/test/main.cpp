//
//  main.cpp
//  test
//
//  Created by 夏俊如 on 4/26/17.
//  Copyright © 2017 Junru Xia. All rights reserved.
//
#include <iostream>
#include <vector>
int main(int argc, const char * argv[]) {
    // insert code here...
    std::string str = "s1_suppkey";
    std::string str1 = "s1";
    std::string str2 = "s";
    std::string delimiter = "_";
    std::string token = str.substr(0, str.find(delimiter));
    str.replace(0,token.length(),str1);
    std::cout << str;
    return 0;
}
