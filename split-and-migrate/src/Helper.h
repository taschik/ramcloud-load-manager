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

	std::vector<std::string> splitString(std::vector<std::string> &result, const std::string &s, const string &delim)
	{
		std::stringstream ss(s);
		std::string item;
		char cdelim = delim[0];

		while (std::getline(ss, item, cdelim))
		{
			result.push_back(item);
		}
		return result;
	}

	string toLowerString(string &input) {
		transform(input.begin(), input.end(), input.begin(),
				std::bind2nd(std::ptr_fun(&std::tolower<char>), std::locale("")));
	return input;
	}
};
