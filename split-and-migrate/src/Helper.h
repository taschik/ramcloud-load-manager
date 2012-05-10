class Helper
{
public:
    Helper();
    virtual ~Helper();

    static int intLength(int integer){
    int end = 0;
        while(integer > 0) {
                integer = integer/10;
                end++;
        }
        return end;
    }

    static char* itoa (int value){

    int length = intLength(value);
    char* key = new char[length];
    sprintf(key,"%d",value);

    return key;
    }
};